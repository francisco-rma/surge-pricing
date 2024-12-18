#!/bin/bash

# Start uvicorn (FastAPI) in the background and wait for it to start
uvicorn app.main:app --host 0.0.0.0 --reload --reload-dir app &

# Get the process ID of uvicorn to ensure it has started properly
UVICORN_PID=$!
echo "Started FastAPI server with PID $UVICORN_PID"

# Start the driver position producer (this will block until the producer stops)
echo "Starting driver position producer..."
python app/driver_position/driver_position_producer.py &

echo "Waiting for the driver position producer to initialize..."
sleep 5  # Adjust this sleep time based on how long it takes the producer to start

# Start the aggregator consumer (this will block until the consumer stops)
echo "Starting the driver position aggregator..."
python app/driver_position/aggregator_consumer.py &

# Wait for the processes to finish
wait $UVICORN_PID

