#!/bin/bash

# init-hive-metastore-db.sh crea una base de datos y un usuario para el Metastore de Hive en PostgreSQL.

# Termina el script si ocurre un error
set -e

# Crear una base de datos y un usuario para el Metastore de Hive
echo "Creating database and user for Hive Metastore..."
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    CREATE DATABASE metastore_db;
    CREATE USER hive;
    ALTER USER hive WITH PASSWORD 'hivepassword';
    GRANT ALL PRIVILEGES ON DATABASE metastore_db TO hive;
EOSQL

echo "Database initialization finished."
