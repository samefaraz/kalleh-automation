#!/bin/bash

# Function to kill background processes on exit
cleanup() {
    echo "Stopping servers..."
    kill $(jobs -p)
    exit
}

trap cleanup SIGINT

echo "Starting Server 1 on port 8000..."
uvicorn app.registration_api1:app --reload --port 8000 &

echo "Starting Server 2 on port 8001..."
uvicorn app.execution_api:app --reload --port 8001 &

# Wait for background processes
wait