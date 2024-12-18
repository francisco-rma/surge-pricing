#!/bin/sh

echo "Starting driver position producer..."
python app/driver_position/producer.py &

echo "Starting order producer..."
python app/orders/producer.py &

# Wait for both background processes to finish
wait
