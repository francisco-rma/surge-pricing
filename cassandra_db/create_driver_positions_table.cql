CREATE KEYSPACE IF NOT EXISTS driver_data
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

USE driver_data;

CREATE TABLE IF NOT EXISTS driver_position (
    driver_id UUID PRIMARY KEY,
    latitude DOUBLE,
    longitude DOUBLE,
    timestamp TIMESTAMP
);
