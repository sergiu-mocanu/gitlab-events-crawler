#!/usr/bin/env bash
set -euo pipefail

# Configuration from environment (with defaults for user & db name)
: "${GLCRAWLER_DB_USER:=glcrawler}"
: "${GLCRAWLER_DB_NAME:=glcrawler_db}"
: "${GLCRAWLER_DB_PASSWORD:?Please set GLCRAWLER_DB_PASSWORD in .env}"

echo "Using role: ${GLCRAWLER_DB_USER}"
echo "Using database: ${GLCRAWLER_DB_NAME}"

############################################
# 1) Create role if it does not exist
############################################
sudo -u postgres psql -v ON_ERROR_STOP=1 <<SQL
DO
\$\$
BEGIN
   IF NOT EXISTS (
       SELECT FROM pg_roles WHERE rolname = '${GLCRAWLER_DB_USER}'
   ) THEN
       CREATE ROLE ${GLCRAWLER_DB_USER} LOGIN PASSWORD '${GLCRAWLER_DB_PASSWORD}';
   END IF;
END
\$\$;
SQL

############################################
# 2) Create database if it does not exist
############################################
if ! sudo -u postgres psql -tAc "SELECT 1 FROM pg_database WHERE datname='${GLCRAWLER_DB_NAME}'" | grep -q 1; then
    echo "Creating database ${GLCRAWLER_DB_NAME} owned by ${GLCRAWLER_DB_USER}..."
    sudo -u postgres createdb -O "${GLCRAWLER_DB_USER}" "${GLCRAWLER_DB_NAME}"
else
    echo "Database ${GLCRAWLER_DB_NAME} already exists, skipping creation."
fi

############################################
# 3) Grant privileges
############################################
sudo -u postgres psql -v ON_ERROR_STOP=1 <<SQL
GRANT ALL PRIVILEGES ON DATABASE ${GLCRAWLER_DB_NAME} TO ${GLCRAWLER_DB_USER};
SQL

echo "✔ DB initialization complete."

