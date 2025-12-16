"""
Shared utilities for database operations and code execution.
This module contains reusable functions for both main.py and execute.py
"""
import os
import uuid
import docker
import tempfile
import json
import logging
import time
import ast
import codecs
import psycopg2
from psycopg2 import pool, errors as psycopg2_errors
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from typing import Optional, Dict, Any, Tuple, List
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)

# Configuration
SANDBOX_IMAGE_NAME = os.getenv("SANDBOX_IMAGE_NAME", "python-execution-sandbox-v1")
CONTAINER_TIMEOUT = int(os.getenv("CONTAINER_TIMEOUT", "300"))

# Database Configuration
DB_CONFIG = {
    "dbname": os.getenv("DB_NAME", "n8ndb"),
    "user": os.getenv("DB_USER", "n8nuser"),
    "password": os.getenv("DB_PASSWORD", "n8npass"),
    "host": os.getenv("DB_HOST", "localhost"),
    "port": os.getenv("DB_PORT", "5432")
}

DB_POOL_MIN_CONN = int(os.getenv("DB_POOL_MIN_CONN", "2"))
DB_POOL_MAX_CONN = int(os.getenv("DB_POOL_MAX_CONN", "10"))

# Retry Configuration
DB_RETRY_MAX_ATTEMPTS = int(os.getenv("DB_RETRY_MAX_ATTEMPTS", "3"))
DB_RETRY_INITIAL_DELAY = float(os.getenv("DB_RETRY_INITIAL_DELAY", "0.5"))
DB_RETRY_MAX_DELAY = float(os.getenv("DB_RETRY_MAX_DELAY", "5.0"))
DB_RETRY_BACKOFF_MULTIPLIER = float(os.getenv("DB_RETRY_BACKOFF_MULTIPLIER", "2.0"))

# Global connection pool
connection_pool: Optional[pool.ThreadedConnectionPool] = None

# Docker client
try:
    docker_client = docker.from_env()
    logger.info("Docker client initialized successfully")
except docker.errors.DockerException as e:
    logger.error(f"Error connecting to Docker: {e}")
    docker_client = None


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

def get_db_pool(): # <-- NEW GETTER FUNCTION
    """Returns the initialized database connection pool."""
    return connection_pool

def close_db_pool():
    """Close all connections in the pool."""
    global connection_pool
    if connection_pool:
        connection_pool.closeall()
        logger.info("Database connection pool closed")


def unescape_code_string(code_str: str) -> str:
    """Unescapes a string that came from a JSON payload."""
    try:
        # return ast.literal_eval(f'"{code_str}"')
        return  codecs.decode(code_str, 'unicode_escape')
    except (ValueError, SyntaxError) as e:
        logger.warning(f"Could not unescape code string. Error: {e}")
        return code_str


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


def get_db_connection() -> Optional[psycopg2.extensions.connection]:
    """Get a database connection from the pool."""
    if not connection_pool:
        raise ConnectionError("Database connection pool is not available")
    
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
            connection_pool.putconn(conn, close=True)
            raise ConnectionError(f"Connection test failed: {e}") from e
        
        return conn
    except (pool.PoolError, ConnectionError) as e:
        logger.error(f"Failed to get database connection: {e}")
        raise


def return_db_connection(conn: psycopg2.extensions.connection):
    """Return a connection to the pool."""
    if conn and not conn.closed:
        try:
            # 💡 CRITICAL: Ensure any uncommitted transaction is rolled back
            # before returning the connection to the pool for reuse.
            if conn.info.transaction_status != psycopg2.extensions.TRANSACTION_STATUS_IDLE:
                 conn.rollback()
        except Exception as rollback_e:
            logger.warning(f"Error during rollback on connection: {rollback_e}. Closing connection.")
            try:
                # If rollback fails, the connection might be broken, so close it.
                connection_pool.putconn(conn, close=True)
                return 
            except Exception:
                pass # Already tried to close
        try:
            connection_pool.putconn(conn)
        except Exception as e:
            logger.error(f"Error returning connection to pool: {e}")
            try:
                conn.close()
            except Exception:
                pass


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


def fetch_function_by_name(function_name: str) -> Optional[Dict[str, Any]]:
    """
    Fetch a function from the database by function_name.
    Returns None if not found.
    """
    if not connection_pool:
        raise ConnectionError("Database connection pool is not available")
    
    conn = None
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        
        query = """
                SELECT 
                    f.function_code, 
                    f.function_name, 
                    f.requirements, 
                    f.execution_interval, 
                    f.last_execution, 
                    STRING_AGG(fc.division, ', ') AS divisions
                FROM 
                    functions AS f 
                JOIN 
                    function_contacts AS fc ON f.function_name = fc.function_name 
                WHERE 
                    f.function_name = %s
                GROUP BY
                    f.function_code, f.function_name, f.requirements, f.execution_interval, f.last_execution;
        """
        
        cur.execute(query, (function_name,))
        row = cur.fetchone()
        
        if not row:
            return None
        
        function_code = row[0]
        function_name_from_db = row[1]
        
        # Extract parameters from function_code and create function_args
        parameters = extract_function_parameters(function_code, function_name_from_db)
        function_args = create_function_args_from_parameters(parameters)
        division_string = row[5]
        division_list = division_string.split(', ')
        # Convert row to dictionary
        function_data = {
            "function_code": function_code,
            "function_name": function_name_from_db,
            "function_args": function_args,  # Dynamically created from function parameters
            "requirements": row[2] if isinstance(row[2], list) else json.loads(row[2]) if row[2] else [],
            "execution_interval": row[3],
            "last_execution": row[4],
            "division": division_list
        }
        
        return function_data


    except psycopg2.Error as e:
        logger.error(f"Database error fetching function '{function_name}': {e}")
        if conn:
            try:
                conn.rollback()
            except Exception:
                pass
        raise
    except Exception as e:
        logger.error(f"Unexpected error fetching function '{function_name}': {e}")
        if conn:
            try:
                conn.rollback()
            except Exception:
                pass
        raise
    finally:
        if conn:
            return_db_connection(conn)

# init_db_pool()
# res = fetch_function_by_name("print_division")
# import pdb; pdb.set_trace()

def update_last_execution(function_name: str, execution_timestamp: Optional[str] = None) -> bool:
    """
    Update the last_execution timestamp for a function.
    If execution_timestamp is None, uses current timestamp.
    Returns True if successful, False otherwise.
    """
    if not connection_pool:
        raise ConnectionError("Database connection pool is not available")
    
    conn = None
    try:
        conn = get_db_connection()
        
        if conn.isolation_level == ISOLATION_LEVEL_AUTOCOMMIT:
            conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_READ_COMMITTED)
        
        cur = conn.cursor()
        
        if execution_timestamp is None:
            execution_timestamp = datetime.utcnow().isoformat()
        
        query = """
        UPDATE functions
        SET last_execution = %s
        WHERE function_name = %s
        RETURNING function_name
        """
        
        cur.execute(query, (execution_timestamp, function_name))
        result = cur.fetchone()
        
        if result:
            conn.commit()
            logger.info(f"Updated last_execution for function '{function_name}' to {execution_timestamp}")
            return True
        else:
            conn.rollback()
            logger.warning(f"Function '{function_name}' not found for last_execution update")
            return False
            
    except psycopg2.Error as e:
        logger.error(f"Database error updating last_execution for '{function_name}': {e}")
        if conn:
            try:
                conn.rollback()
            except Exception:
                pass
        raise
    except Exception as e:
        logger.error(f"Unexpected error updating last_execution for '{function_name}': {e}")
        if conn:
            try:
                conn.rollback()
            except Exception:
                pass
        raise
    finally:
        if conn:
            return_db_connection(conn)


def save_execution_history(
    function_name: str,
    success: bool,
    output: Optional[str] = None,
    error: Optional[str] = None,
    execution_time_ms: Optional[int] = None
) -> Optional[int]:
    """
    Save execution history to database.
    Creates execution_history table if it doesn't exist.
    Returns the execution_id if successful, None otherwise.
    """
    if not connection_pool:
        logger.warning("Database connection pool not available, skipping execution history")
        return None
    
    conn = None
    try:
        conn = get_db_connection()
        
        if conn.isolation_level == ISOLATION_LEVEL_AUTOCOMMIT:
            conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_READ_COMMITTED)
        
        cur = conn.cursor()
        
        # Create execution_history table if it doesn't exist
        create_table_query = """
        CREATE TABLE IF NOT EXISTS execution_history (
            execution_id SERIAL PRIMARY KEY,
            function_name VARCHAR(255) NOT NULL,
            execution_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            success BOOLEAN NOT NULL,
            output TEXT,
            error TEXT,
            execution_time_ms INTEGER,
            FOREIGN KEY (function_name) REFERENCES functions(function_name) ON DELETE CASCADE
        );
        CREATE INDEX IF NOT EXISTS idx_execution_history_function_name ON execution_history(function_name);
        CREATE INDEX IF NOT EXISTS idx_execution_history_timestamp ON execution_history(execution_timestamp);
        """
        
        cur.execute(create_table_query)
        conn.commit()
        
        # Insert execution record
        insert_query = """
        INSERT INTO execution_history (
            function_name,
            success,
            output,
            error,
            execution_time_ms
        ) VALUES (%s, %s, %s, %s, %s)
        RETURNING execution_id
        """
        
        cur.execute(insert_query, (
            function_name,
            success,
            output[:10000] if output else None,  # Limit output size
            error[:10000] if error else None,   # Limit error size
            execution_time_ms
        ))
        
        result = cur.fetchone()
        conn.commit()
        
        if result:
            execution_id = result[0]
            logger.info(f"Saved execution history for '{function_name}' with ID {execution_id}")
            return execution_id
        else:
            logger.warning(f"Failed to save execution history for '{function_name}'")
            return None
            
    except psycopg2.Error as e:
        logger.error(f"Database error saving execution history for '{function_name}': {e}")
        if conn:
            try:
                conn.rollback()
            except Exception:
                pass
        return None
    except Exception as e:
        logger.error(f"Unexpected error saving execution history for '{function_name}': {e}")
        if conn:
            try:
                conn.rollback()
            except Exception:
                pass
        return None
    finally:
        if conn:
            return_db_connection(conn)


def execute_code_in_container(
    function_code: str,
    function_name: str,
    function_args: Dict[str, Any],
    requirements: list, 
    divisions: list
) -> Dict[str, Any]:
    """
    Execute Python code in a Docker container for multiple divisions.

    Returns:
        A dictionary with the overall success status and detailed results for each division.
        Example:
        {
            'overall_success': False,
            'results': {
                'division_A': {'success': True, 'output': '...', 'error': None},
                'division_B': {'success': False, 'output': None, 'error': '...'}
            }
        }
    """
    all_results = {}
    overall_success = True

    if not docker_client:
        # This is a fatal error for all divisions
        return {
            'overall_success': False,
            'results': {div: {'success': False, 'output': None, 'error': "Docker service is unavailable"} for div in divisions}
        }

    for division in divisions:    
        start_time = time.time()
        container_name = f"sandbox-{uuid.uuid4()}"
        
        with tempfile.TemporaryDirectory() as temp_dir:
            code_path = os.path.join(temp_dir, "code.py")
            args_path = os.path.join(temp_dir, "args.json")
            requirements_path = os.path.join(temp_dir, "requirements.txt")
            
            try:
                # Write function arguments
                with open(args_path, "w") as f:
                    json.dump(function_args, f)
                
                # Write requirements
                with open(requirements_path, "w") as f:
                    f.write("\n".join(requirements))
                
                # Create wrapper script
                wrapper_script = f"""
import json
import sys

# --- User's Function Code ---
{function_code}

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
                error_msg = f"Failed to prepare execution files for division '{division}': {str(e)}"
                logger.error(error_msg, exc_info=True)
                # --- CHANGE: Store the error and continue to the next division ---
                overall_success = False
                all_results[division] = {'success': False, 'output': None, 'error': error_msg}
                continue # Move to the next division        
            # Run Docker container
            try:
                log_stream = docker_client.containers.run(
                    image=SANDBOX_IMAGE_NAME,
                    name=container_name,
                    volumes={temp_dir: {'bind': '/app', 'mode': 'rw'}},
                    mem_limit="512m",
                    nano_cpus=1000_000_000,
                    network_mode="bridge",
                    # timeout=CONTAINER_TIMEOUT,
                    remove=True,
                    stderr=True,
                    stdout=True,
                )
                
                full_output = log_stream.decode('utf-8', errors='replace').strip()
                user_output_marker = "--- Running user code ---"
                
                execution_time_ms = int((time.time() - start_time) * 1000)
                
                if user_output_marker in full_output:
                    user_output = full_output.split(user_output_marker, 1)[1].strip()
                    logger.info(f"Function '{function_name}' for division '{division}' executed successfully in {execution_time_ms}ms")
                    # --- CHANGE: Store the successful result ---
                    all_results[division] = {'success': True, 'output': user_output, 'error': None}
                else:
                    error_output = full_output
                    logger.warning(f"Function '{function_name}' for division '{division}' execution failed: {error_output}")
                    # --- CHANGE: Store the failure result and mark overall failure ---
                    overall_success = False
                    all_results[division] = {'success': False, 'output': None, 'error': error_output}
                    
            except docker.errors.ContainerError as e:
                execution_time_ms = int((time.time() - start_time) * 1000)
                error_parts = []
                if hasattr(e, 'stderr') and e.stderr:
                    try:
                        error_parts.append(f"stderr: {e.stderr.decode('utf-8', errors='replace').strip()}")
                    except (AttributeError, UnicodeDecodeError):
                        error_parts.append(f"stderr: {str(e.stderr)}")
                # ... (rest of your error parsing) ...
                error_output = " | ".join(error_parts) if error_parts else str(e)
                logger.error(f"Container error for function '{function_name}' division '{division}': {error_output}")
                # --- CHANGE: Store the container error and mark overall failure ---
                overall_success = False
                all_results[division] = {'success': False, 'output': None, 'error': f"Container execution failed: {error_output}"}
            
            # ... (Handle other docker exceptions like ImageNotFound, APIError, etc. in the same way) ...
            except Exception as e:
                error_msg = f"Unexpected error executing function '{function_name}' for division '{division}': {str(e)}"
                logger.error(error_msg, exc_info=True)
                overall_success = False
                all_results[division] = {'success': False, 'output': None, 'error': error_msg}

    # --- CHANGE: After the loop, return the complete summary ---
    return {
        'overall_success': overall_success,
        'results': all_results
    }