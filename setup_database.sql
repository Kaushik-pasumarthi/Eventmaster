-- Setup script for PostgreSQL database
-- Run this with: psql -U postgres -f setup_database.sql

-- Create user
CREATE USER prowess_user WITH PASSWORD 'prowess_password';

-- Create database
CREATE DATABASE corporate_actions_db OWNER prowess_user;

-- Grant privileges
GRANT ALL PRIVILEGES ON DATABASE corporate_actions_db TO prowess_user;

-- Connect to new database and grant schema permissions
\c corporate_actions_db
GRANT ALL ON SCHEMA public TO prowess_user;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO prowess_user;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO prowess_user;

-- Verify setup
\du
\l
