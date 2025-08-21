-- Create source schema for raw data
CREATE SCHEMA IF NOT EXISTS source;
-- Set up permissions for postgres user
GRANT USAGE ON SCHEMA source TO postgres;
GRANT CREATE ON SCHEMA source TO postgres;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA source TO postgres;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA source TO postgres;
