#!/bin/bash
set -e

# Function to wait for postgres
wait_for_postgres() {
    echo "Waiting for PostgreSQL to be ready..."
    while ! pg_isready -h postgres -U airflow; do
        sleep 1
    done
    echo "PostgreSQL is ready!"
}

# Function to initialize database
init_db() {
    echo "Initializing Airflow database..."
    airflow db init
    
    # Create admin user if it doesn't exist
    echo "Creating admin user..."
    airflow users create \
        --username admin \
        --firstname Admin \
        --lastname User \
        --role Admin \
        --email admin@example.com \
        --password admin || true
}

# Wait for PostgreSQL
wait_for_postgres

# Initialize database only for webserver
if [ "$1" = "webserver" ]; then
    init_db
fi

# Execute the original command
exec airflow "$@" 