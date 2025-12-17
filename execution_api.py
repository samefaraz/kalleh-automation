"""
Execution endpoint for scheduled function execution.
This endpoint is called by n8n after checking the database for functions ready to execute.
"""
import os
import json
import logging
import time
import psycopg2
from psycopg2 import errors as psycopg2_errors
from datetime import datetime, timezone, timedelta
from typing import Optional, List
from fastapi import APIRouter, HTTPException, Request, status, Header, Depends
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field, RootModel
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.util import get_remote_address
from slowapi.errors import RateLimitExceeded
from dotenv import load_dotenv

# Import shared utilities
from utils import (
    init_db_pool,
    close_db_pool,
    fetch_function_by_name,
    update_last_execution,
    save_execution_history,
    execute_code_in_container,
    docker_client,
    connection_pool,
    get_db_pool,
    get_db_connection,
    return_db_connection
)

load_dotenv()

# Logging Configuration
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

#intialize router
router=APIRouter()

# Rate Limiting Configuration
EXECUTE_RATE_LIMIT_PER_MINUTE = int(os.getenv("EXECUTE_RATE_LIMIT_PER_MINUTE", "60"))
EXECUTE_RATE_LIMIT_PER_HOUR = int(os.getenv("EXECUTE_RATE_LIMIT_PER_HOUR", "1000"))

# API Key for authentication (optional but recommended)
API_KEY = os.getenv("EXECUTE_API_KEY", None)  # Set in .env for production

# Initialize rate limiter
limiter = Limiter(key_func=get_remote_address)

# Pydantic Models
class ExecuteRequest(BaseModel):
    """Request model for executing a function."""
    function_name: str = Field(..., example="my_func", description="Name of the function to execute")
    api_key: Optional[str] = Field(None, example="your-api-key", description="Optional API key for authentication")


class ExecuteResponse(BaseModel):
    """Response model for execution results."""
    success: bool
    function_name: str
    division: str
    output: Optional[str] = None
    error: Optional[str] = None
    execution_time_ms: Optional[int] = None
    execution_id: Optional[int] = None
    message: Optional[str] = None


# --- 1. Data Models ---

class N8nBatchItem(BaseModel):
    """Model for a single item in the n8n batch payload."""
    function_name: str = Field(..., example="mockFunc", description="The name of the function to execute.")
    execution_interval: int = Field(..., example=12, description="The scheduled interval for execution (hours).")
    # Using datetime allows Pydantic to automatically parse the ISO string from n8n
    last_execution: datetime = Field(..., description="The timestamp of the last execution.")

class BatchExecuteRequest(RootModel):
    """
    RootModel is required here because the input is a JSON Array (List),
    not a JSON Object (Dictionary).
    """
    root: List[N8nBatchItem]

    def __iter__(self):
        return iter(self.root)

    def __getitem__(self, item):
        return self.root[item]

class ExecuteResponse(BaseModel):
    """Response model for a single execution result."""
    success: bool
    function_name: str
    division: str
    output: Optional[str] = None
    error: Optional[str] = None
    execution_time_ms: Optional[int] = None
    execution_id: Optional[int] = None
    message: str

class BatchExecuteResponse(BaseModel):
    """Response model for the aggregate batch results."""
    total: int
    successful: int
    failed: int
    results: List[ExecuteResponse]

# Authentication helper
def verify_api_key(api_key: Optional[str] = None) -> bool:
    """
    Verify API key if authentication is enabled.
    Returns True if authentication is disabled or key is valid.
    """
    if API_KEY is None:
        # Authentication disabled
        return True
    
    if api_key is None:
        return False
    
    return api_key == API_KEY

def get_db():
    """
    Dependency that yields a database connection and handles its return to the pool.
    """
    conn = None
    try:
        # Use the connection manager from utils.py
        conn = get_db_connection() 
        
        if conn is None:
            # This should only happen if get_db_connection fails, which raises an exception, 
            # but we keep this check for safety.
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="Database connection pool is unavailable or failed to provide connection."
            )
        
        # Pass the connection to the endpoint function
        yield conn 
        
    except Exception as e:
        logger.error(f"Database dependency error: {e}", exc_info=True)
        # Re-raise as an HTTPException to control the API response
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Database connection error: {e}"
        )
    finally:
        # This block runs AFTER the endpoint finishes, ensuring cleanup
        if conn:
            return_db_connection(conn)


@router.get("/health")
async def health_check():
    """
    Health check endpoint to verify service status.
    """
    current_pool = get_db_pool()
    connection_pool = current_pool
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
            from utils import get_db_connection, return_db_connection
            conn = get_db_connection()
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
                    return_db_connection(conn)
        except Exception as e:
            health_status["database"] = f"error: {str(e)}"
            health_status["status"] = "degraded"
    
    status_code = status.HTTP_503_SERVICE_UNAVAILABLE if health_status["status"] == "degraded" else status.HTTP_200_OK
    return JSONResponse(content=health_status, status_code=status_code)


@router.post("/execute", response_model=ExecuteResponse)
@limiter.limit(f"{EXECUTE_RATE_LIMIT_PER_MINUTE}/minute")
@limiter.limit(f"{EXECUTE_RATE_LIMIT_PER_HOUR}/hour")
async def execute_function(
    request: Request,
    execute_request: ExecuteRequest,
    x_api_key: Optional[str] = Header(None, alias="X-API-Key")
):
    """
    Execute a function by name from the database.
    
    This endpoint:
    1. Fetches the function from the database
    2. Executes it in a Docker container
    3. Updates last_execution timestamp
    4. Saves execution history
    5. Returns the result
    
    Rate limited: {EXECUTE_RATE_LIMIT_PER_MINUTE} requests per minute, {EXECUTE_RATE_LIMIT_PER_HOUR} per hour.
    """
    start_time = time.time()
    function_name = execute_request.function_name
    
    # Verify API key (if provided in header or request body)
    api_key = x_api_key or execute_request.api_key
    if not verify_api_key(api_key):
        logger.warning(f"Unauthorized execution attempt for function '{function_name}'")
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or missing API key"
        )
    
    logger.info(f"Received execution request for function '{function_name}'")
    
    # Check Docker availability
    if not docker_client:
        error_msg = "Docker service is unavailable"
        logger.error(error_msg)
        raise HTTPException(status_code=503, detail=error_msg)
    
    # Fetch function from database
    try:
        function_data = fetch_function_by_name(function_name)
        if not function_data:
            error_msg = f"Function '{function_name}' not found in database"
            logger.warning(error_msg)
            return ExecuteResponse(
                success=False,
                function_name=function_name,
                error=error_msg,
                message="Function not found"
            )
    except Exception as e:
        error_msg = f"Database error fetching function '{function_name}': {str(e)}"
        logger.error(error_msg, exc_info=True)
        raise HTTPException(status_code=500, detail=error_msg)
    
    # Execute the function
    try:
        success, output, error = execute_code_in_container(
            function_code=function_data["function_code"],
            function_name=function_name,
            function_args=function_data["function_args"],
            requirements=function_data["requirements"],
            divisions=function_data["division"]
        )
        
        execution_time_ms = int((time.time() - start_time) * 1000)
        execution_timestamp = datetime.utcnow().isoformat()
        
        # Update last_execution timestamp (only on success)
        if success:
            try:
                update_last_execution(function_name, execution_timestamp)
            except Exception as e:
                logger.warning(f"Failed to update last_execution for '{function_name}': {e}")
        
        # Save execution history
        execution_id = None
        try:
            execution_id = save_execution_history(
                function_name=function_name,
                success=success,
                output=output,
                error=error,
                execution_time_ms=execution_time_ms
            )
        except Exception as e:
            logger.warning(f"Failed to save execution history for '{function_name}': {e}")
        
        # Return response
        if success:
            logger.info(f"Function '{function_name}' executed successfully in {execution_time_ms}ms")
            return ExecuteResponse(
                success=True,
                function_name=function_name,
                output=output,
                execution_time_ms=execution_time_ms,
                execution_id=execution_id,
                message="Execution completed successfully"
            )
        else:
            logger.error(f"Function '{function_name}' execution failed: {error}")
            return ExecuteResponse(
                success=False,
                function_name=function_name,
                error=error,
                execution_time_ms=execution_time_ms,
                execution_id=execution_id,
                message="Execution failed"
            )
            
    except Exception as e:
        error_msg = f"Unexpected error executing function '{function_name}': {str(e)}"
        logger.error(error_msg, exc_info=True)
        execution_time_ms = int((time.time() - start_time) * 1000)
        
        # Save execution history for the error
        try:
            execution_id = save_execution_history(
                function_name=function_name,
                success=False,
                error=error_msg,
                execution_time_ms=execution_time_ms
            )
        except Exception:
            pass
        
        raise HTTPException(status_code=500, detail=error_msg)


@router.post("/execute/batch", response_model=BatchExecuteResponse)
@limiter.limit(f"{EXECUTE_RATE_LIMIT_PER_MINUTE}/minute")
@limiter.limit(f"{EXECUTE_RATE_LIMIT_PER_HOUR}/hour")
async def execute_batch(
    request: Request,
    batch_request: BatchExecuteRequest
    # x_api_key: Optional[str] = Header(None, alias="X-API-Key")
):
    """
    Execute multiple functions in batch from an n8n List input.
    """
    # --- AUTHENTICATION FIX ---
    # Since the body is a List, we cannot look for 'api_key' inside batch_request.
    # Authentication must be done via the Header.
    # if not x_api_key or not verify_api_key(x_api_key):
    #     logger.warning("Unauthorized batch execution attempt")
    #     raise HTTPException(
    #         status_code=status.HTTP_401_UNAUTHORIZED,
    #         detail="Invalid or missing X-API-Key header"
    #     )
    
    # 1. Access the list of items from the root
    n8n_items: List[N8nBatchItem] = batch_request.root
    
    logger.info(f"Received batch execution request for {len(n8n_items)} functions")
    
    if not docker_client:
        raise HTTPException(status_code=503, detail="Docker service is unavailable")
    
    all_results = []
    total_successful = 0
    total_failed = 0
    
    # 2. Iterate directly over the Pydantic models
    for item in n8n_items:
        function_name = item.function_name
        # Note: You can also access item.execution_interval and item.last_execution here if needed
        
        try:
            # Fetch function
            function_data = fetch_function_by_name(function_name)
            if not function_data:
                results.append(ExecuteResponse(
                    success=False,
                    function_name=function_name,
                    error=f"Function '{function_name}' not found in database",
                    message="Function not found"
                ))
                total_failed += 1
                continue
            
            # Execute
            start_time = time.time()
            execution_summary = execute_code_in_container(
                function_code=function_data["function_code"],
                function_name=function_name,
                function_args=function_data["function_args"],
                requirements=function_data["requirements"],
                divisions=function_data["division"]
            )
            
            execution_time_ms = int((time.time() - start_time) * 1000)
            execution_timestamp = datetime.utcnow().isoformat()
            
            overall_success = execution_summary['overall_success']
            detailed_results = execution_summary['results']
            # Update last_execution (only on success)
            successful_divisions_output = []
            failed_divisions_errors = []

            detailed_results = execution_summary['results']
            for division, result in detailed_results.items():
                
                # Update last_execution for each successful division
                if result['success']:
                    try:
                        # You might want to update this logic to track per-division execution
                        update_last_execution(function_name, datetime.utcnow().isoformat())
                    except Exception as e:
                        logger.warning(f"Failed to update last_execution for '{function_name}': {e}")

                execution_id = None
                try:
                    execution_id = save_execution_history(
                        function_name=function_name,
                        success=result['success'],
                        output=result['output'],
                        error=result['error'],
                        execution_time_ms=execution_time_ms
                    )
                except Exception as e:
                    logger.warning(f"Failed to save execution history for '{function_name}' division '{division}': {e}")

                # Create a separate response for this division
                response_item = ExecuteResponse(
                    success=result['success'],
                    function_name=function_name,
                    division=division, # <-- Set the division here
                    output=result['output'],
                    error=result['error'],
                    execution_time_ms=execution_time_ms,
                    execution_id=execution_id,
                    message="Execution completed successfully" if result['success'] else "Execution failed"
                )
                all_results.append(response_item)

                if result['success']:
                    total_successful += 1
                else:
                    total_failed += 1

        except Exception as e:
            # Handle unexpected errors for a function
            error_msg = f"Unexpected error executing function '{function_name}': {str(e)}"
            logger.error(error_msg, exc_info=True)
            # This is tricky, as we don't know the divisions. You might need to pass them in.
            # For now, let's just add one generic failure.
            all_results.append(ExecuteResponse(
                success=False,
                function_name=function_name,
                division="Unknown", # Or handle this differently
                error=error_msg,
                message="Unexpected error"
            ))
            total_failed += 1

    logger.info(f"Batch execution completed: {total_successful} successful, {total_failed} failed")
    return BatchExecuteResponse(
        total=len(all_results),
        successful=total_successful,
        failed=total_failed,
        results=all_results
    )


@router.get("/functions/ready")
async def get_ready_functions(
    # x_api_key: Optional[str] = Header(None, alias="X-API-Key"),
    conn: psycopg2.extensions.connection = Depends(get_db)
):
    """
    Get list of functions that are ready to execute based on execution_interval.
    
    A function is ready if:
    1. last_execution is NULL/empty.
    2. The time passed since last_execution >= execution_interval (in hours).
    """
    # Verify API key
    # if not verify_api_key(x_api_key):
    #     raise HTTPException(
    #         status_code=status.HTTP_401_UNAUTHORIZED,
    #         detail="Invalid or missing API key"
    #     )
    
    # 🎯 1. Get current time, explicitly in UTC and timezone-aware
    now_utc = datetime.now(timezone.utc)
    
    try:
        cur = conn.cursor()
        
        # Query functions (no change to the database query needed, still fetch all)
        query = """
        SELECT 
            function_name,
            execution_interval,
            last_execution
        FROM functions
        ORDER BY function_name
        """
        
        cur.execute(query)
        rows = cur.fetchall()
        
        ready_functions = []
        
        # 🎯 2. Iterate and filter based on time
        for row in rows:
            function_name = row[0]
            execution_interval = row[1] # Assumed to be an integer or float representing hours
            last_exec_db = row[2]       # Can be None, a string, or a datetime object
            
            is_ready = False
            last_exec_dt = None # Initialize datetime object
            
            # --- Condition 1: Missing/Empty last_execution ---
            if not last_exec_db or execution_interval is None:
                is_ready = True
                
            # --- Condition 2: Time passed is sufficient ---
            elif execution_interval is not None and execution_interval > 0:
                try:
                    # If it's a string (e.g., from DB or row[2]), parse it
                    if isinstance(last_exec_db, str):
                        # datetime.fromisoformat handles the 'T' and the timezone offset (+03:30)
                        last_exec_dt = datetime.fromisoformat(last_exec_db)
                    # If psycopg2 already converted it to a datetime object
                    elif isinstance(last_exec_db, datetime):
                        last_exec_dt = last_exec_db
                    
                    if last_exec_dt:
                        # Convert the required interval (hours) into a timedelta object
                        required_timedelta = timedelta(hours=execution_interval)
                        
                        # Time passed since last execution
                        # Convert last_exec_dt to UTC for safe comparison if it's not already
                        if last_exec_dt.tzinfo is not None:
                             time_passed = now_utc - last_exec_dt.astimezone(timezone.utc)
                        else:
                            # If naive, assume it's UTC for legacy compatibility, though highly discouraged
                            time_passed = now_utc - last_exec_dt.replace(tzinfo=timezone.utc)

                        if time_passed >= required_timedelta:
                            is_ready = True
                            
                except ValueError as e:
                    logger.warning(f"Skipping function '{function_name}'. Failed to parse last_execution timestamp: {e}")
                    # If parsing fails, we skip execution check and do not mark as ready.
                    
            if is_ready:
                # Reformat last_execution for the response payload
                if last_exec_db:
                    # Use the actual parsed datetime object if available, otherwise fallback
                    if last_exec_dt and hasattr(last_exec_dt, 'isoformat'):
                        # Ensure the output is in ISO format
                        last_exec_str = last_exec_dt.isoformat()
                    else:
                        last_exec_str = str(last_exec_db)
                else:
                    last_exec_str = None
                
                ready_functions.append({
                    "function_name": function_name,
                    "execution_interval": execution_interval,
                    "last_execution": last_exec_str
                })
        
        return {
            "count": len(ready_functions),
            "functions": ready_functions
        }
        
    except psycopg2_errors.Error as e:
        error_msg = f"Database error fetching ready functions: {str(e)}"
        logger.error(error_msg, exc_info=True)
        raise HTTPException(status_code=500, detail=error_msg)
    except Exception as e:
        error_msg = f"Unexpected error fetching ready functions: {str(e)}"
        logger.error(error_msg, exc_info=True)
        raise HTTPException(status_code=500, detail=error_msg)
    # The connection is automatically returned to the pool by the `get_db` dependency's `finally` block.