import os
import re
import uuid
import docker
import tempfile
import ast
import json
import logging
import time
import functools
import psycopg2
import pandas as pd # ADDED: For Excel handling
import io # ADDED: For file stream handling
from datetime import datetime, timezone
from psycopg2 import pool, errors as psycopg2_errors
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from fastapi import FastAPI, HTTPException, Request, status, UploadFile, File, Form
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field, field_validator
from typing import List, Optional, Dict, Any, Callable
from dotenv import load_dotenv
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.util import get_remote_address
from slowapi.errors import RateLimitExceeded

# --- Load Environment Variables ---
load_dotenv()

# --- Logging Configuration ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

# --- Configuration ---
SANDBOX_IMAGE_NAME = "python-execution-sandbox-v1"

# --- Database Configuration ---
DB_CONFIG = {
    "dbname": os.getenv("DB_NAME", "n8ndb"),
    "user": os.getenv("DB_USER", "n8nuser"),
    "password": os.getenv("DB_PASSWORD", "n8npass"),
    "host": os.getenv("DB_HOST", "localhost"),
    "port": os.getenv("DB_PORT", "5432")
}

# --- Connection Pool Configuration ---
DB_POOL_MIN_CONN = int(os.getenv("DB_POOL_MIN_CONN", "2"))
DB_POOL_MAX_CONN = int(os.getenv("DB_POOL_MAX_CONN", "10"))

# --- Retry Configuration ---
DB_RETRY_MAX_ATTEMPTS = int(os.getenv("DB_RETRY_MAX_ATTEMPTS", "3"))
DB_RETRY_INITIAL_DELAY = float(os.getenv("DB_RETRY_INITIAL_DELAY", "0.5"))
DB_RETRY_MAX_DELAY = float(os.getenv("DB_RETRY_MAX_DELAY", "5.0"))
DB_RETRY_BACKOFF_MULTIPLIER = float(os.getenv("DB_RETRY_BACKOFF_MULTIPLIER", "2.0"))

# --- Rate Limiting Configuration ---
RATE_LIMIT_PER_MINUTE = int(os.getenv("RATE_LIMIT_PER_MINUTE", "10"))
RATE_LIMIT_PER_HOUR = int(os.getenv("RATE_LIMIT_PER_HOUR", "100"))

# --- Container Configuration ---
CONTAINER_TIMEOUT = int(os.getenv("CONTAINER_TIMEOUT", "300"))

# Global connection pool
connection_pool: Optional[pool.ThreadedConnectionPool] = None

# --- Rate Limiter Initialization ---
limiter = Limiter(key_func=get_remote_address)

# --- FastAPI App Initialization ---
app = FastAPI(
    title="Python Function Execution Sandbox",
    description="An API to execute Python functions per Excel row and save contacts.",
    version="3.2.0"
)

app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)

# --- Docker Client Initialization ---
try:
    docker_client = docker.from_env()
    logger.info("Docker client initialized successfully")
except docker.errors.DockerException as e:
    logger.error(f"Error connecting to Docker: {e}")
    docker_client = None

# --- Helper Functions (Unchanged parts hidden for brevity, but crucial parts kept) ---

def build_sandbox_image():
    """Builds the sandbox Docker image if it doesn't exist."""
    try:
        docker_client.images.get(SANDBOX_IMAGE_NAME)
    except docker.errors.ImageNotFound:
        logger.info(f"Image '{SANDBOX_IMAGE_NAME}' not found. Building...")
        docker_client.images.build(path=".", tag=SANDBOX_IMAGE_NAME, rm=True)

def init_db_pool():
    global connection_pool
    try:
        connection_pool = pool.ThreadedConnectionPool(
            minconn=DB_POOL_MIN_CONN, maxconn=DB_POOL_MAX_CONN, **DB_CONFIG
        )
        logger.info("Database connection pool initialized")
    except Exception as e:
        logger.error(f"Failed to initialize database connection pool: {e}")

def close_db_pool():
    if connection_pool:
        connection_pool.closeall()

@app.on_event("startup")
async def on_startup():
    if docker_client:
        build_sandbox_image()
    init_db_pool()
    # Create the contact table if it doesn't exist
    create_contact_table()

@app.on_event("shutdown")
async def on_shutdown():
    close_db_pool()

def parse_requirements(requirements):
    if requirements is None:
        return []
    
    if isinstance(requirements, str):
        # First try JSON parsing (for JSON arrays)
        try:
            parsed = json.loads(requirements)
            if isinstance(parsed, list):
                return parsed
        except json.JSONDecodeError:
            pass
        
        # Then try AST literal evaluation (for Python list syntax)
        try:
            parsed = ast.literal_eval(requirements)
            if isinstance(parsed, list):
                return parsed
        except (ValueError, SyntaxError):
            pass
        
        # If neither worked, treat as comma-separated values or single requirement
        # Split by comma and strip whitespace
        if ',' in requirements:
            return [req.strip() for req in requirements.split(',') if req.strip()]
        else:
            return [requirements] if requirements else []
    
    # If already a list, return as-is
    if isinstance(requirements, list):
        return requirements
    
    raise ValueError("requirements must be a list, JSON array, or comma-separated string.")

def unescape_code_string(code_str: str) -> str:
    try:
        return ast.literal_eval(f'"{code_str}"')
    except (ValueError, SyntaxError):
        return code_str

def extract_function_name(function_code: str) -> Optional[str]:
    unescaped_code = unescape_code_string(function_code)
    try:
        tree = ast.parse(unescaped_code)
        for node in ast.walk(tree):
            if isinstance(node, ast.FunctionDef):
                return node.name
        return None
    except SyntaxError:
        return None

def extract_function_parameters(function_code: str, function_name: str) -> List[str]:
    unescaped_code = unescape_code_string(function_code)
    try:
        tree = ast.parse(unescaped_code)
        for node in ast.walk(tree):
            if isinstance(node, ast.FunctionDef) and node.name == function_name:
                all_args = [arg.arg for arg in node.args.args]
                all_kwonlyargs = [arg.arg for arg in node.args.kwonlyargs]
                return [arg for arg in all_args + all_kwonlyargs if arg != 'self']
        return []
    except SyntaxError:
        return []

def create_function_args(parameters: List[str], division_value: Any) -> Dict[str, Any]:
    """
    Creates arguments. If the function has a parameter named 'division' (case-insensitive),
    inject the Excel value. Otherwise, set to None.
    """
    args = {}
    for param in parameters:
        if param.lower() == 'division':
            args[param] = division_value
        else:
            args[param] = None
    return args

def clean_n8n_time(time_str: Optional[str]) -> Optional[str]:
    """
    Converts '[DateTime: 2025-12-14T13:48:42.148+03:30]' 
    to '2025-12-14T13:48:42.148+03:30'
    """
    if not time_str:
        return None
    
    # Remove [DateTime: and ] and whitespace
    cleaned = re.sub(r'\[DateTime:\s*|\]', '', time_str).strip()
    return cleaned

# --- Retry Logic ---
def is_retryable_error(error: Exception) -> bool:
    if isinstance(error, psycopg2_errors.OperationalError): return True
    if isinstance(error, pool.PoolError): return True
    return False

def retry_db_operation(max_attempts=3, delay=1):
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            for attempt in range(max_attempts):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    if not is_retryable_error(e) or attempt == max_attempts - 1:
                        raise
                    time.sleep(delay)
        return wrapper
    return decorator

def get_db_connection():
    if not connection_pool:
        raise ConnectionError("Pool not available")
    return connection_pool.getconn()

# --- DB Functions ---

def create_contact_table():
    """Ensures the function_contacts table exists."""
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS function_contacts (
                    id SERIAL PRIMARY KEY,
                    function_name VARCHAR(255) NOT NULL,
                    division VARCHAR(255),
                    email VARCHAR(255),
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    CONSTRAINT unique_contact UNIQUE (function_name, division, email)
                );
            """)
        conn.commit()
    except Exception as e:
        logger.error(f"Error creating table: {e}")
        conn.rollback()
    finally:
        connection_pool.putconn(conn)

@retry_db_operation()
def save_function_metadata(
    function_code: str,
    function_name: str,
    requirements: List[str],
    execution_interval: int,
    last_execution: Optional[str]
):
    """Saves the function definition itself."""
    conn = get_db_connection()
    if conn.isolation_level == ISOLATION_LEVEL_AUTOCOMMIT:
        conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_READ_COMMITTED)
    try:
        cur = conn.cursor()
        query = """
        INSERT INTO functions (function_code, function_name, requirements, execution_interval, last_execution)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (function_name) DO UPDATE SET
            function_code = EXCLUDED.function_code,
            requirements = EXCLUDED.requirements,
            execution_interval = EXCLUDED.execution_interval,
            last_execution = EXCLUDED.last_execution
        RETURNING function_name;
        """
        cur.execute(query, (function_code, function_name, json.dumps(requirements), execution_interval, last_execution))
        conn.commit()
        return "Function Metadata Saved"
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        connection_pool.putconn(conn)

@retry_db_operation()
def save_contact_to_db(function_name: str, division: str, email: str):
    """Saves the successful execution contact info."""
    conn = get_db_connection()
    try:
        cur = conn.cursor()
        query = """
        INSERT INTO function_contacts (function_name, division, email)
        VALUES (%s, %s, %s)
        ON CONFLICT (function_name, division, email) DO NOTHING
        RETURNING function_name;
        """
        cur.execute(query, (function_name, str(division), str(email)))
        conn.commit()
    except Exception as e:
        conn.rollback()
        logger.error(f"Failed to save contact: {e}")
        # We don't raise here to avoid stopping the whole response for one contact fail
    finally:
        connection_pool.putconn(conn)

# --- NEW Endpoint ---

@app.post("/execute")
@limiter.limit(f"{RATE_LIMIT_PER_MINUTE}/minute")
async def execute_code(
    request: Request,
    file: UploadFile = File(..., description="Excel/CSV file with 'Division' and 'Email' columns"),
    function: str = Form(..., description="Python function code"),
    requirements: str = Form("[]", description="JSON string list of requirements"),
    execution_interval: int = Form(24),
    time_val: Optional[str] = Form(None, alias="time")
):
    if not docker_client:
        raise HTTPException(status_code=503, detail="Docker service is unavailable.")

    cleaned_last_execution = clean_n8n_time(time_val)
    if not cleaned_last_execution:
        cleaned_last_execution = datetime.now(timezone.utc)

    # 1. Parse Requirements
    parsed_requirements = parse_requirements(requirements)

    # 2. Process File (Smart Loader)
    try:
        contents = await file.read()
        file_obj = io.BytesIO(contents)
        filename = file.filename.lower()
        
        # A. Helper to find the correct header row
        def find_header_and_load(loader_func):
            # Read first few rows without a header to scan them
            df_preview = loader_func(file_obj, header=None, nrows=10)
            
            header_idx = -1
            # Search for the row containing 'Division' and 'Email'
            for idx, row in df_preview.iterrows():
                row_str = [str(val).strip().lower() for val in row.values]
                if 'division' in row_str and 'email' in row_str:
                    header_idx = idx
                    break
            
            # Reset file pointer to beginning before reloading
            file_obj.seek(0)
            
            if header_idx != -1:
                # Reload with the correct header row
                return loader_func(file_obj, header=header_idx)
            else:
                # If not found, try default (row 0)
                return loader_func(file_obj)

        # B. Choose Loader based on extension
        if filename.endswith('.csv'):
            df = find_header_and_load(pd.read_csv)
        else:
            df = find_header_and_load(pd.read_excel)
        
        # C. Normalize Headers (strip whitespace and lowercase for checking)
        df.columns = df.columns.astype(str).str.strip()
        
        # Case-insensitive column check
        # We create a map of {lowercase_name: actual_name} to find them easily
        col_map = {col.lower(): col for col in df.columns}
        
        if 'division' not in col_map or 'email' not in col_map:
             return JSONResponse(
                status_code=400, 
                content={
                    "success": False, 
                    "error": f"Columns 'Division' and 'Email' not found. Found columns: {list(df.columns)}"
                }
            )
            
        # Standardize column names for the loop below
        df.rename(columns={col_map['division']: 'Division', col_map['email']: 'Email'}, inplace=True)

    except Exception as e:
        logger.error(f"File processing error: {e}")
        return JSONResponse(status_code=400, content={"success": False, "error": f"Invalid File: {str(e)}"})

    # --- Pre-execution Setup ---
    unescaped_function_code = unescape_code_string(function)
    function_name = extract_function_name(unescaped_function_code)
    
    if not function_name:
        return JSONResponse(status_code=400, content={"success": False, "error": "No function definition found."})

    parameters = extract_function_parameters(unescaped_function_code, function_name)
    results = []
    
    # Save Metadata
    # try:
    #     save_function_metadata(function, function_name, parsed_requirements, execution_interval, time_val)
    # except Exception as e:
    #     logger.error(f"Could not save function metadata: {e}")

    # 3. Execution Loop
    for index, row in df.iterrows():
        division = row.get('Division')
        email = row.get('Email')
        
        # Skip empty rows
        if pd.isna(division) or pd.isna(email):
            continue

        row_args = create_function_args(parameters, division)

        with tempfile.TemporaryDirectory() as temp_dir:
            # Write args and reqs
            with open(os.path.join(temp_dir, "args.json"), "w") as f:
                json.dump(row_args, f)
            with open(os.path.join(temp_dir, "requirements.txt"), "w") as f:
                f.write("\n".join(parsed_requirements))
            
            # Wrapper Script
            wrapper_script = f"""
import json
import sys
# --- User Code ---
{unescaped_function_code}
# -----------------

if __name__ == "__main__":
    try:
        with open('args.json', 'r') as f:
            kwargs = json.load(f)
        
        # Find function
        func = locals().get('{function_name}')
        if not func: raise ValueError("Function not found")

        print("--- Running user code ---")
        res = func(**kwargs)
        print(res)
    except Exception as e:
        print(f"Error: {{e}}", file=sys.stderr)
        sys.exit(1)
"""
            with open(os.path.join(temp_dir, "code.py"), "w") as f:
                f.write(wrapper_script)

            # Run Docker
            container_name = f"sandbox-{uuid.uuid4()}"
            exec_success = False
            output_log = ""
            
            try:
                log_stream = docker_client.containers.run(
                    image=SANDBOX_IMAGE_NAME,
                    name=container_name,
                    volumes={temp_dir: {'bind': '/app', 'mode': 'rw'}},
                    mem_limit="128m",
                    remove=True,
                    stderr=True,
                    stdout=True
                )
                full_output = log_stream.decode('utf-8', errors='replace').strip()
                if "--- Running user code ---" in full_output:
                    output_log = full_output.split("--- Running user code ---", 1)[1].strip()
                    exec_success = True
                else:
                    output_log = full_output
                    exec_success = False

            except Exception as e:
                output_log = str(e)
                exec_success = False

            # Database Save Logic
            db_status_metadata = "Skipped"
            db_status = "Skipped"
            if exec_success:
                try:
                    db_status_metadata = save_function_metadata(function, function_name, parsed_requirements, execution_interval, cleaned_last_execution )
                except Exception as e:
                    db_status_metadata = f"DB Error: {str(e)}"
                    logger.error(f"Could not save function metadata: {e}")
                try:
                    save_contact_to_db(function_name, division, email)
                    db_status = "Saved Contact"
                except Exception as e:
                    db_status = f"DB Error: {str(e)}"

            results.append({
                "division": division,
                "email": email,
                "success": exec_success,
                "output": output_log,
                "db_status": db_status
            })

    return {
        "function_name": function_name,
        "db_status_metadata": db_status_metadata,
        "total_rows": len(results),
        "results": results
    }