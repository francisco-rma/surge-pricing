#!/bin/bash

# Start uvicorn (FastAPI) in the background and wait for it to start
uvicorn app.main:app --host 0.0.0.0 --reload --reload-dir app &

# Get the process ID of uvicorn to ensure it has started properly
UVICORN_PID=$!
echo "Started FastAPI server with PID $UVICORN_PID"

# Wait for the processes to finish
wait $UVICORN_PID

