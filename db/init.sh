#!/bin/bash
set -e

echo "🔧 Initializing PostgreSQL with dynamic credentials..."

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" <<-EOSQL
    -- Create ETL user if not exists
    DO \$\$
    BEGIN
        IF NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = '${ETL_USERNAME}') THEN
            CREATE ROLE ${ETL_USERNAME} WITH LOGIN PASSWORD '${ETL_PASSWORD}';
        END IF;
    END
    \$\$;

    -- Connect to database
    \c ${POSTGRES_DB};

    -- Create table if needed
    CREATE TABLE IF NOT EXISTS ${TABLE_NAME} (
        id SERIAL PRIMARY KEY,
        start_time TIMESTAMP,
        end_time TIMESTAMP,
        organization TEXT,
        site_name TEXT,
        pollutant TEXT,
        raw_value FLOAT,
        unit TEXT,
        quality_code TEXT,
        validity INTEGER,
        city TEXT,
        longitude FLOAT,
        latitude FLOAT
    );

    -- Change ownership to ETL user
    ALTER TABLE ${TABLE_NAME} OWNER TO ${ETL_USERNAME};

    -- ✅ Permissions for ETL user
    GRANT CONNECT ON DATABASE ${POSTGRES_DB} TO ${ETL_USERNAME};
    GRANT USAGE ON SCHEMA public TO ${ETL_USERNAME};
    GRANT CREATE ON SCHEMA public TO ${ETL_USERNAME};  -- <== ajout important
    GRANT SELECT, INSERT, UPDATE, DELETE ON ${TABLE_NAME} TO ${ETL_USERNAME};
    GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO ${ETL_USERNAME};

    REVOKE CREATE ON SCHEMA public FROM PUBLIC;
EOSQL

echo "✅ Database '${POSTGRES_DB}' initialized, ETL user '${ETL_USERNAME}' can now manage indexes."