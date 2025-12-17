import os
import logging
from fastapi import FastAPI
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.util import get_remote_address
from slowapi.errors import RateLimitExceeded

# Import Shared Utils
from utils import init_db_pool, close_db_pool, docker_client

# Import Routers
from execution_api import router as execution_router
from registration_api1 import router as registration_router, build_sandbox_image, create_functions_table, create_contact_table

# Logging Config
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

# Initialize Main App
app = FastAPI(
    title="Unified Function Execution Service",
    description="Combines scheduled execution and manual file-based registration.",
    version="4.0.0"
)

# Setup Rate Limiting (Global State)
limiter = Limiter(key_func=get_remote_address)
app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)

# --- Lifecycle Events ---

@app.on_event("startup")
async def startup_event():
    """Initialize shared resources."""
    logger.info("Starting up Unified Service...")
    
    # 1. Initialize DB Pool (Shared by both routers)
    init_db_pool()
    
    # 2. Docker Setup
    if docker_client:
        build_sandbox_image() # From registration_api1 logic
    else:
        logger.warning("Docker client is not available.")
        
    # 3. Database Migration/Setup
    create_functions_table()
    create_contact_table() # From registration_api1 logic

@app.on_event("shutdown")
async def shutdown_event():
    """Cleanup shared resources."""
    logger.info("Shutting down...")
    close_db_pool()

# --- Router Registration ---

# 1. Mount Scheduled Execution API
# Original routes: /execute (POST json), /execute/batch
# Now available at: /api/scheduled/execute, /api/scheduled/execute/batch
app.include_router(
    execution_router, 
    prefix="/api/scheduled", 
    tags=["Scheduled Execution"]
)

# 2. Mount Registration API
# Original routes: /execute (POST file)
# Now available at: /api/registration/execute
app.include_router(
    registration_router, 
    prefix="/api/registration", 
    tags=["File Registration"]
)

# 3. Health Check (Global)
@app.get("/health")
async def root_health():
    return {"status": "ok", "services": ["scheduled", "registration"]}

