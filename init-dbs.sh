#!/bin/bash
set -e[1]

# Split the comma-separated string into an array
if [ -n "$POSTGRES_MULTIPLE_DATABASES" ]; then
    echo "Requested creation of databases: $POSTGRES_MULTIPLE_DATABASES"
    
    for db in $(echo $POSTGRES_MULTIPLE_DATABASES | tr ',' ' '); do
        echo "Checking if database '$db' exists..."
        
        # Check if database exists
        if psql -U "$POSTGRES_USER" -lqt | cut -d \| -f 1 | grep -qw "$db"; then
            echo "Database '$db' already exists."
        else
            echo "Creating database '$db'..."
            psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" <<-EOSQL
                CREATE DATABASE $db;
                GRANT ALL PRIVILEGES ON DATABASE $db TO $POSTGRES_USER;
EOSQL
        fi
    done
    echo "Multiple databases processed."
fi
