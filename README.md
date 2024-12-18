Surge Pricing
Overview

This project is focused on implementing a surge pricing model, simulating pricing adjustments based on demand fluctuations.
Features

    Dynamic pricing logic: Adjusts prices based on factors such as demand and supply.
    Real-time data processing: Utilizes Python for backend operations.
    Dockerized environment: Easily deployable using Docker.

Setup

    Clone the repository.

git clone https://github.com/denisefavila/surge-pricing.git

Install dependencies:

poetry install

Start the services using Docker Compose:

    docker-compose up --build redis
    docker-compose up --build redis_producer
    docker-compose up --build redis_aggregator
    docker-compose up --build redis_orders_aggregator
    docker-compose up --build app

Contributing

    Fork the repository.
    Create a new branch.
    Make your changes and submit a pull request.

License

This project is licensed under the MIT License.
