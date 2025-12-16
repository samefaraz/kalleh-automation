import os
import uuid
import docker
import tempfile
import ast
import json
import logging
import time
import functools
import psycopg2
from psycopg2 import pool, errors as psycopg2_errors
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from fastapi import FastAPI, HTTPException, Request, status
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
# Load database credentials from .env fileß
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
CONTAINER_TIMEOUT = int(os.getenv("CONTAINER_TIMEOUT", "300"))  # 5 minutes default

# Global connection pool (initialized on startup)
connection_pool: Optional[pool.Threade dConnectionPool] = None

# --- Rate Limiter Initialization ---
limiter = Limiter(key_func=get_remote_address)

# --- Pydantic Models for Request/Response ---
class CodeExecutionRequest(BaseModel):
    function: str = Field(..., example="def my_func(data, api_key=None):\n    return f'Data: {data}, Key: {api_key}'", description="The Python function code")
    requirements: Optional[List[str]] = Field(default_factory=list, example=["requests==2.28.1"], description="Optional list of Python package requirements")
    execution_interval: Optional[int] = Field(None, example=24, description="How often the function should run (in hours). Defaults to 24 if not provided.")
    time: Optional[str] = Field(None, example="DateTime: 2025-12-03T15:13:30.301+03:30", description="n8n now object for last_execution timestamp")

    @field_validator('requirements', mode='before')
    @classmethod
    def parse_requirements(cls, v):
        if v is None:
            return []
        
        if isinstance(v, str):
            # First try JSON parsing (for JSON arrays)
            try:
                parsed = json.loads(v)
                if isinstance(parsed, list):
                    return parsed
            except json.JSONDecodeError:
                pass
            
            # Then try AST literal evaluation (for Python list syntax)
            try:
                parsed = ast.literal_eval(v)
                if isinstance(parsed, list):
                    return parsed
            except (ValueError, SyntaxError):
                pass
            
            # If neither worked, treat as comma-separated values or single requirement
            # Split by comma and strip whitespace
            if ',' in v:
                return [req.strip() for req in v.split(',') if req.strip()]
            else:
                return [v] if v else []
        
        # If already a list, return as-is
        if isinstance(v, list):
            return v
        
        raise ValueError("requirements must be a list, JSON array, or comma-separated string.")
    
    @field_validator('execution_interval', mode='before')
    @classmethod
    def set_default_execution_interval(cls, v):
        if v is None or v == "":
            return 24
        return v

class CodeExecutionResponse(BaseModel):
    success: bool
    output: Optional[str] = None
    error: Optional[str] = None
    db_status: Optional[str] = None # ADDED: To inform user if DB save worked

# --- FastAPI App Initialization ---
app = FastAPI(
    title="Python Function Execution Sandbox",
    description="An API to safely execute a user-submitted Python function and save to DB on success.",
    version="3.1.0"
)

# Initialize rate limiter
app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)

# --- Docker Client Initialization ---
try:
    docker_client = docker.from_env()
    logger.info("Docker client initialized successfully")
except docker.errors.DockerException as e:
    logger.error(f"Error connecting to Docker: {e}")
    logger.error("Please ensure Docker is installed and running.")
    docker_client = None

def build_sandbox_image():
    """Builds the sandbox Docker image if it doesn't exist."""
    try:
        docker_client.images.get(SANDBOX_IMAGE_NAME)
        logger.info(f"Image '{SANDBOX_IMAGE_NAME}' already exists.")
    except docker.errors.ImageNotFound:
        logger.info(f"Image '{SANDBOX_IMAGE_NAME}' not found. Building...")
        docker_client.images.build(path=".", tag=SANDBOX_IMAGE_NAME, rm=True)
        logger.info("Build complete.")

def init_db_pool():
    """Initialize the database connection pool."""
    global connection_pool
    try:
        connection_pool = pool.ThreadedConnectionPool(
            minconn=DB_POOL_MIN_CONN,
            maxconn=DB_POOL_MAX_CONN,
            **DB_CONFIG
        )
        logger.info(f"Database connection pool initialized (min={DB_POOL_MIN_CONN}, max={DB_POOL_MAX_CONN})")
        
        # Test the connection pool
        test_conn = connection_pool.getconn()
        if test_conn:
            try:
                with test_conn.cursor() as test_cur:
                    test_cur.execute("SELECT 1")
                    test_cur.fetchone()
                logger.info("Database connection pool test successful")
            except Exception as e:
                logger.error(f"Database connection pool test failed: {e}")
            finally:
                connection_pool.putconn(test_conn)
    except Exception as e:
        logger.error(f"Failed to initialize database connection pool: {e}")
        connection_pool = None

def close_db_pool():
    """Close all connections in the pool."""
    global connection_pool
    if connection_pool:
        connection_pool.closeall()
        logger.info("Database connection pool closed")

@app.on_event("startup")
async def on_startup():
    """Initialize services on application startup."""
    if docker_client:
        build_sandbox_image()
    init_db_pool()

@app.on_event("shutdown")
async def on_shutdown():
    """Cleanup on application shutdown."""
    close_db_pool()
    logger.info("Application shutdown complete")

def unescape_code_string(code_str: str) -> str:
    """Unescapes a string that came from a JSON payload."""
    try:
        return ast.literal_eval(f'"{code_str}"')
    except (ValueError, SyntaxError) as e:
        logger.warning(f"Could not unescape code string. Error: {e}")
        return code_str

def function_exists(function_code: str, function_name: str) -> bool:
    """Check if a function with the given name exists in the code."""
    unescaped_code = unescape_code_string(function_code)
    try:
        tree = ast.parse(unescaped_code)
        for node in ast.walk(tree):
            if isinstance(node, ast.FunctionDef) and node.name == function_name:
                return True
        return False
    except SyntaxError:
        return False

def has_parameter(function_code: str, function_name: str, parameter_name: str) -> bool:
    """Parses Python code to check if a given function has a specific parameter."""
    unescaped_code = unescape_code_string(function_code)
    try:
        tree = ast.parse(unescaped_code)
        for node in ast.walk(tree):
            if isinstance(node, ast.FunctionDef) and node.name == function_name:
                all_args = [arg.arg for arg in node.args.args] + [arg.arg for arg in node.args.kwonlyargs]
                return parameter_name in all_args
        return False 
    except SyntaxError:
        return False

def extract_function_name(function_code: str) -> Optional[str]:
    """Extract the first function name from the code."""
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
    """Extract all parameter names from a function."""
    unescaped_code = unescape_code_string(function_code)
    try:
        tree = ast.parse(unescaped_code)
        for node in ast.walk(tree):
            if isinstance(node, ast.FunctionDef) and node.name == function_name:
                # Get all parameters (args, kwonlyargs, but exclude *args and **kwargs)
                all_args = [arg.arg for arg in node.args.args]
                all_kwonlyargs = [arg.arg for arg in node.args.kwonlyargs]
                # Exclude 'self' if present (for methods)
                all_params = [arg for arg in all_args + all_kwonlyargs if arg != 'self']
                return all_params
        return []
    except SyntaxError:
        return []

def create_function_args_from_parameters(parameters: List[str]) -> Dict[str, Any]:
    """Create a function_args dictionary with default None values for all parameters."""
    return {param: None for param in parameters}

# --- Retry Logic for Database Operations ---
def is_retryable_error(error: Exception) -> bool:
    """Check if an error is retryable."""
    if isinstance(error, psycopg2_errors.OperationalError):
        # Connection errors, timeouts, etc. are retryable
        error_code = getattr(error, 'pgcode', None)
        # Common retryable error codes:
        # 08003 - connection_does_not_exist
        # 08006 - connection_failure
        # 08001 - sqlclient_unable_to_establish_sqlconnection
        # 08004 - sqlserver_rejected_establishment_of_sqlconnection
        # 08007 - transaction_resolution_unknown
        # 57P01 - admin_shutdown
        # 57P02 - crash_shutdown
        # 57P03 - cannot_connect_now
        retryable_codes = ['08003', '08006', '08001', '08004', '08007', '57P01', '57P02', '57P03']
        if error_code in retryable_codes:
            return True
        # Check error message for common retryable patterns
        error_msg = str(error).lower()
        retryable_patterns = [
            'connection', 'timeout', 'network', 'unreachable', 
            'refused', 'broken pipe', 'closed', 'lost connection'
        ]
        return any(pattern in error_msg for pattern in retryable_patterns)
    
    if isinstance(error, pool.PoolError):
        # Pool errors are retryable
        return True
    
    if isinstance(error, psycopg2_errors.InterfaceError):
        # Interface errors (connection lost, etc.) are retryable
        return True
    
    # Transaction errors that might be retryable
    if isinstance(error, psycopg2_errors.TransactionRollbackError):
        # Deadlock, serialization failures - might be retryable
        error_code = getattr(error, 'pgcode', None)
        # 40001 - serialization_failure
        # 40P01 - deadlock_detected
        retryable_codes = ['40001', '40P01']
        return error_code in retryable_codes
    
    return False

def retry_db_operation(
    max_attempts: int = DB_RETRY_MAX_ATTEMPTS,
    initial_delay: float = DB_RETRY_INITIAL_DELAY,
    max_delay: float = DB_RETRY_MAX_DELAY,
    backoff_multiplier: float = DB_RETRY_BACKOFF_MULTIPLIER
):
    """Decorator for retrying database operations with exponential backoff."""
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            last_exception = None
            delay = initial_delay
            
            for attempt in range(1, max_attempts + 1):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    last_exception = e
                    
                    # Check if error is retryable
                    if not is_retryable_error(e):
                        logger.error(f"Non-retryable error in {func.__name__}: {e}")
                        raise
                    
                    # Don't retry on last attempt
                    if attempt >= max_attempts:
                        logger.error(
                            f"Max retry attempts ({max_attempts}) reached for {func.__name__}. "
                            f"Last error: {e}"
                        )
                        raise
                    
                    logger.warning(
                        f"Retryable error in {func.__name__} (attempt {attempt}/{max_attempts}): {e}. "
                        f"Retrying in {delay:.2f}s..."
                    )
                    time.sleep(delay)
                    delay = min(delay * backoff_multiplier, max_delay)
            
            # Should never reach here, but just in case
            if last_exception:
                raise last_exception
        
        return wrapper
    return decorator

def get_db_connection_with_retry() -> Optional[psycopg2.extensions.connection]:
    """Get a database connection from the pool with retry logic."""
    if not connection_pool:
        raise ConnectionError("Database connection pool is not available")
    
    last_exception = None
    delay = DB_RETRY_INITIAL_DELAY
    
    for attempt in range(1, DB_RETRY_MAX_ATTEMPTS + 1):
        try:
            conn = connection_pool.getconn()
            if conn is None:
                raise ConnectionError("Failed to get connection from pool")
            
            # Test the connection
            try:
                with conn.cursor() as test_cur:
                    test_cur.execute("SELECT 1")
                    test_cur.fetchone()
            except (psycopg2_errors.OperationalError, psycopg2_errors.InterfaceError) as e:
                # Connection is bad, put it back and try again
                connection_pool.putconn(conn, close=True)
                raise
            
            return conn
        except (pool.PoolError, ConnectionError, psycopg2_errors.OperationalError, 
                psycopg2_errors.InterfaceError) as e:
            last_exception = e
            
            if attempt >= DB_RETRY_MAX_ATTEMPTS:
                logger.error(
                    f"Failed to get database connection after {DB_RETRY_MAX_ATTEMPTS} attempts: {e}"
                )
                raise ConnectionError(f"Failed to get database connection: {e}") from e
            
            logger.warning(
                f"Failed to get database connection (attempt {attempt}/{DB_RETRY_MAX_ATTEMPTS}): {e}. "
                f"Retrying in {delay:.2f}s..."
            )
            time.sleep(delay)
            delay = min(delay * DB_RETRY_BACKOFF_MULTIPLIER, DB_RETRY_MAX_DELAY)
    
    if last_exception:
        raise ConnectionError(f"Failed to get database connection: {last_exception}") from last_exception
    return None

# --- NEW FUNCTION: Save to Postgres ---
@retry_db_operation()
def save_to_postgres(
    function_code: str,
    function_name: str,
    requirements: List[str],
    execution_interval: int,
    last_execution: Optional[str]
) -> str:
    """Inserts the function data into PostgreSQL using connection pool with retry logic."""
    if not connection_pool:
        error_msg = "Database connection pool is not available"
        logger.error(error_msg)
        raise ConnectionError(error_msg)
    
    conn = None
    cur = None
    
    try:
        # Get connection from pool with retry
        conn = get_db_connection_with_retry()
        
        # Ensure we're in a transaction (not autocommit mode)
        if conn.isolation_level == ISOLATION_LEVEL_AUTOCOMMIT:
            conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_READ_COMMITTED)
        
        cur = conn.cursor()

        # SQL Query matching simplified table structure
        insert_query = """
        INSERT INTO functions (
            function_code, 
            function_name, 
            requirements, 
            execution_interval,
            last_execution
        ) VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (function_name)
        DO UPDATE SET
            function_code = EXCLUDED.function_code,
            requirements = EXCLUDED.requirements,
            execution_interval = EXCLUDED.execution_interval,
            last_execution = EXCLUDED.last_execution 
            RETURNING function_name;
        """

        # Convert List to JSON String for JSONB column
        reqs_json = json.dumps(requirements)

        cur.execute(insert_query, (
            function_code,
            function_name,
            reqs_json,
            execution_interval,
            last_execution
        ))
        
        result = cur.fetchone()
        if result is None:
            error_msg = "No result returned from database insert"
            logger.error(error_msg)
            conn.rollback()
            raise ValueError(error_msg)
        
        new_pk = result[0]
        conn.commit()
        logger.info(f"Successfully saved function '{function_name}' to database with ID: {new_pk}")
        return f"Saved to DB with ID: {new_pk}"

    except psycopg2_errors.IntegrityError as e:
        # Integrity errors (constraint violations) - rollback and don't retry
        if conn:
            try:
                conn.rollback()
            except Exception as rollback_error:
                logger.error(f"Error during rollback: {rollback_error}")
        error_msg = f"Database integrity error: {str(e)}"
        logger.error(error_msg, exc_info=True)
        raise ValueError(error_msg) from e
    
    except (psycopg2_errors.OperationalError, psycopg2_errors.InterfaceError) as e:
        # Connection errors - rollback and let retry decorator handle it
        if conn:
            try:
                # Mark connection as bad so it gets removed from pool
                conn.rollback()
            except Exception:
                # Connection is already broken, can't rollback
                pass
        error_msg = f"Database connection error: {str(e)}"
        logger.error(error_msg, exc_info=True)
        raise ConnectionError(error_msg) from e
    
    except psycopg2_errors.TransactionRollbackError as e:
        # Transaction errors (deadlocks, serialization failures) - rollback and retry
        if conn:
            try:
                conn.rollback()
            except Exception as rollback_error:
                logger.error(f"Error during rollback: {rollback_error}")
        error_msg = f"Database transaction error: {str(e)}"
        logger.warning(error_msg)
        raise  # Let retry decorator handle it
    
    except psycopg2.Error as e:
        # Other PostgreSQL errors - rollback
        if conn:
            try:
                conn.rollback()
            except Exception as rollback_error:
                logger.error(f"Error during rollback: {rollback_error}")
        error_msg = f"PostgreSQL error: {str(e)}"
        logger.error(error_msg, exc_info=True)
        raise ValueError(error_msg) from e
    
    except Exception as e:
        # Unexpected errors - rollback
        if conn:
            try:
                conn.rollback()
            except Exception as rollback_error:
                logger.error(f"Error during rollback: {rollback_error}")
        error_msg = f"Unexpected database error: {str(e)}"
        logger.error(error_msg, exc_info=True)
        raise ValueError(error_msg) from e
    
    finally:
        # Clean up cursor
        if cur:
            try:
                cur.close()
            except Exception as e:
                logger.warning(f"Error closing cursor: {e}")
        
        # Return connection to pool
        if conn:
            try:
                # Check if connection is still valid before returning to pool
                if conn.closed:
                    logger.warning("Connection is closed, not returning to pool")
                else:
                    connection_pool.putconn(conn)
            except Exception as e:
                logger.error(f"Error returning connection to pool: {e}")
                # If we can't return it, try to close it
                try:
                    conn.close()
                except Exception:
                    pass

@app.get("/health")
async def health_check():
    """
    Health check endpoint to verify service status.
    """
    health_status = {
        "status": "healthy",
        "docker": "available" if docker_client else "unavailable",
        "database": "available" if connection_pool else "unavailable",
        "timestamp": time.time()
    }
    
    # Check Docker connectivity
    if docker_client:
        try:
            docker_client.ping()
            health_status["docker"] = "available"
        except Exception as e:
            health_status["docker"] = f"error: {str(e)}"
            health_status["status"] = "degraded"
    
    # Check database connectivity
    if connection_pool:
        try:
            conn = connection_pool.getconn()
            if conn:
                try:
                    with conn.cursor() as cur:
                        cur.execute("SELECT 1")
                        cur.fetchone()
                    health_status["database"] = "available"
                except Exception as e:
                    health_status["database"] = f"error: {str(e)}"
                    health_status["status"] = "degraded"
                finally:
                    connection_pool.putconn(conn)
        except Exception as e:
            health_status["database"] = f"error: {str(e)}"
            health_status["status"] = "degraded"
    
    # Determine overall status
    if health_status["docker"] != "available" or health_status["database"] != "available":
        status_code = status.HTTP_503_SERVICE_UNAVAILABLE if health_status["status"] == "degraded" else status.HTTP_200_OK
    else:
        status_code = status.HTTP_200_OK
    
    return JSONResponse(content=health_status, status_code=status_code)

@app.post("/execute", response_model=CodeExecutionResponse)
@limiter.limit(f"{RATE_LIMIT_PER_MINUTE}/minute")
@limiter.limit(f"{RATE_LIMIT_PER_HOUR}/hour")
async def execute_code(request: Request, execution_request: CodeExecutionRequest):
    """
    Validates, Executes, and if successful, saves to DB.
    Rate limited: {RATE_LIMIT_PER_MINUTE} requests per minute, {RATE_LIMIT_PER_HOUR} per hour.
    
    Simplified registration: Only requires function code, optional requirements, execution_interval, and time.
    Automatically extracts function name and parameters from the function code.
    """
    if not docker_client:
        logger.error("Docker service is unavailable")
        raise HTTPException(status_code=503, detail="Docker service is unavailable.")

    unescaped_function_code = unescape_code_string(execution_request.function)

    # 1. Extract function name from code
    function_name = extract_function_name(unescaped_function_code)
    if not function_name:
        error_msg = "Validation failed: No function found in the provided code."
        logger.warning(error_msg)
        return CodeExecutionResponse(
            success=False,
            error=error_msg
        )
    
    logger.info(f"Received execution request for function '{function_name}'")

    # 2. Extract parameters from function
    parameters = extract_function_parameters(unescaped_function_code, function_name)
    logger.info(f"Function '{function_name}' has parameters: {parameters}")
    
    # 3. Create function_args from detected parameters
    function_args = create_function_args_from_parameters(parameters)
    
    # 4. Set execution_interval (default to 24 if not provided)
    execution_interval = execution_request.execution_interval if execution_request.execution_interval is not None else 24
    
    # 5. Set last_execution from time field
    last_execution = execution_request.time

    # 3. Execution Setup
    with tempfile.TemporaryDirectory() as temp_dir:
        code_path = os.path.join(temp_dir, "code.py")
        args_path = os.path.join(temp_dir, "args.json")
        requirements_path = os.path.join(temp_dir, "requirements.txt")

        try:
            with open(args_path, "w") as f:
                json.dump(function_args, f)

            with open(requirements_path, "w") as f:
                f.write("\n".join(execution_request.requirements))

            wrapper_script = f"""
import json
import sys

# --- User's Function Code ---
{unescaped_function_code}

# --- Execution Logic ---
if __name__ == "__main__":
    try:
        with open('args.json', 'r') as f:
            kwargs = json.load(f)
        
        func_to_call = locals().get('{function_name}')
        if not func_to_call:
            raise NameError(f"Function '{function_name}' not found.")

        print("--- Running user code ---")
        result = func_to_call(**kwargs)
        print(result)

    except Exception as e:
        print(f"Error during function execution: {{e}}", file=sys.stderr)
        sys.exit(1)
"""
            with open(code_path, "w") as f:
                f.write(wrapper_script)
        except Exception as e:
            error_msg = f"Failed to prepare execution files: {str(e)}"
            logger.error(error_msg, exc_info=True)
            return CodeExecutionResponse(success=False, error=error_msg)

        container_name = f"sandbox-{uuid.uuid4()}"
        logger.info(f"Starting container '{container_name}' for function '{function_name}'")
        
        # 4. Run Docker Container
        try:
            log_stream = docker_client.containers.run(
                image=SANDBOX_IMAGE_NAME,
                name=container_name,
                volumes={temp_dir: {'bind': '/app', 'mode': 'rw'}},
                mem_limit="128m",
                nano_cpus=500_000_000,
                network_mode="bridge",
                remove=True,
                stderr=True,
                stdout=True,
            )

            full_output = log_stream.decode('utf-8', errors='replace').strip()
            user_output_marker = "--- Running user code ---"

            if user_output_marker in full_output:
                user_output = full_output.split(user_output_marker, 1)[1].strip()
                logger.info(f"Function '{function_name}' executed successfully")
                
                # --- SUCCESS: NOW SAVE TO DB ---
                try:
                    db_result = save_to_postgres(
                        function_code=execution_request.function,
                        function_name=function_name,
                        requirements=execution_request.requirements,
                        execution_interval=execution_interval,
                        last_execution=last_execution
                    )
                    return CodeExecutionResponse(
                        success=True, 
                        output=user_output,
                        db_status=db_result
                    )
                except (ConnectionError, ValueError) as db_error:
                    # Database operation failed even after retries
                    error_msg = str(db_error)
                    logger.error(f"Failed to save function '{function_name}' to database: {error_msg}")
                    return CodeExecutionResponse(
                        success=True,  # Code execution succeeded
                        output=user_output,
                        db_status=f"Database Error: {error_msg}"
                    )
                except Exception as db_error:
                    # Unexpected database error
                    error_msg = f"Unexpected database error: {str(db_error)}"
                    logger.error(f"Unexpected error saving function '{function_name}' to database: {error_msg}", exc_info=True)
                    return CodeExecutionResponse(
                        success=True,  # Code execution succeeded
                        output=user_output,
                        db_status=f"Database Error: {error_msg}"
                    )
            else:
                logger.warning(f"Function '{function_name}' execution failed: {full_output}")
                return CodeExecutionResponse(success=False, error=full_output)

        except docker.errors.ContainerError as e:
            # Container exited with non-zero status
            error_parts = []
            if hasattr(e, 'stderr') and e.stderr:
                try:
                    error_parts.append(f"stderr: {e.stderr.decode('utf-8', errors='replace').strip()}")
                except (AttributeError, UnicodeDecodeError):
                    error_parts.append(f"stderr: {str(e.stderr)}")
            if hasattr(e, 'stdout') and e.stdout:
                try:
                    error_parts.append(f"stdout: {e.stdout.decode('utf-8', errors='replace').strip()}")
                except (AttributeError, UnicodeDecodeError):
                    error_parts.append(f"stdout: {str(e.stdout)}")
            if hasattr(e, 'exit_status'):
                error_parts.append(f"exit_status: {e.exit_status}")
            
            error_output = " | ".join(error_parts) if error_parts else str(e)
            logger.error(f"Container error for function '{function_name}': {error_output}")
            return CodeExecutionResponse(success=False, error=f"Container execution failed: {error_output}")
        
        except docker.errors.ImageNotFound as e:
            error_msg = f"Docker image '{SANDBOX_IMAGE_NAME}' not found. Please build the image first."
            logger.error(f"Image not found for function '{function_name}': {error_msg}")
            return CodeExecutionResponse(success=False, error=error_msg)
        
        except docker.errors.APIError as e:
            error_msg = f"Docker API error: {str(e)}"
            logger.error(f"Docker API error for function '{function_name}': {error_msg}", exc_info=True)
            return CodeExecutionResponse(success=False, error=f"Docker API error: {error_msg}")
        
        except TimeoutError as e:
            error_msg = f"Container execution timed out after {CONTAINER_TIMEOUT} seconds"
            logger.error(f"Container timeout for function '{function_name}': {error_msg}")
            return CodeExecutionResponse(success=False, error=error_msg)
        
        except Exception as e:
            error_msg = f"Unexpected error: {str(e)}"
            logger.error(f"Unexpected error executing function '{function_name}': {error_msg}", exc_info=True)
            return CodeExecutionResponse(success=False, error=error_msg)