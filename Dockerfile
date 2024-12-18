ARG PYTHON_VERSION=3.10.1
FROM python:${PYTHON_VERSION}-slim as base

# Prevents Python from writing pyc files.
ENV PYTHONDONTWRITEBYTECODE=1

# Keeps Python from buffering stdout and stderr to avoid situations where
# the application crashes without emitting any logs due to buffering.
ENV PYTHONUNBUFFERED=1

WORKDIR /app

# Install system dependencies for psycopg2
RUN apt-get update && apt-get install -y gcc libpq-dev bash && rm -rf /var/lib/apt/lists/*

# Install Uvicorn and Poetry
RUN pip install --no-cache-dir uvicorn poetry

# Pre-install psycopg2
RUN pip install --no-cache-dir psycopg2

# Copy poetry configuration files
COPY pyproject.toml poetry.lock ./

# Install dependencies using Poetry
RUN poetry config virtualenvs.create false \
    && poetry install --no-interaction --no-ansi

# Copy the source code into the container.
COPY . /app/

# Copy the shell script to start both processes
COPY start_services.sh /app/start_services.sh
RUN chmod +x /app/start_services.sh

# Expose the port that the application listens on.
EXPOSE 8000
EXPOSE 8050

# Set the default command to run the shell script, which starts both uvicorn and the driver position producer
CMD ["/app/start_services.sh"]
