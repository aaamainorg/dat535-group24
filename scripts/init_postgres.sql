-- =============================================================================
-- One-time PostgreSQL setup for the DAT535 Group 24 pipeline.
--
-- Creates:
--   * the login role  norgespris  (password given on the command line, never stored here)
--   * two databases   norgespris_dev  and  norgespris_prod   (one per environment)
--   * three schemas   bronze, silver, gold   in each database (the Medallion layers)
--     (the gold stage creates gold_stage and gold_previous itself - the role owns the database)
--
-- Run it ONCE as a PostgreSQL superuser, from the repo root:
--   macOS (Homebrew):  psql -d postgres -v pw='choose-a-password' -f scripts/init_postgres.sql
--   Linux (apt):       sudo -u postgres psql -v pw='choose-a-password' < scripts/init_postgres.sql
--   Docker:            docker exec -i norgespris-postgres psql -U postgres -v pw='choose-a-password' < scripts/init_postgres.sql
-- Then put the same password in .env as NORGESPRIS_PG_PASSWORD.
-- =============================================================================

-- Stop at the first error (do not run the rest of the script).
\set ON_ERROR_STOP on

-- The pipeline logs in as this role. :'pw' inserts the password from "-v pw=..." as a quoted text value.
CREATE ROLE norgespris LOGIN PASSWORD :'pw';                    -- the login role

-- One database per environment, owned by the pipeline role (same idea as the landing folders /tmp/norgespris-landing/dev and /prod).
CREATE DATABASE norgespris_dev OWNER norgespris;                -- database for dev
CREATE DATABASE norgespris_prod OWNER norgespris;               -- database for prod

-- Schemas = Medallion layers. Owned by the pipeline role, so Spark can create and replace tables in them.
-- Continue inside the dev database:
\connect norgespris_dev
CREATE SCHEMA IF NOT EXISTS bronze AUTHORIZATION norgespris;    -- raw tables
CREATE SCHEMA IF NOT EXISTS silver AUTHORIZATION norgespris;    -- clean tables
CREATE SCHEMA IF NOT EXISTS gold   AUTHORIZATION norgespris;    -- star schema + dashboard tables

-- Continue inside the prod database:
\connect norgespris_prod
CREATE SCHEMA IF NOT EXISTS bronze AUTHORIZATION norgespris;    -- raw tables
CREATE SCHEMA IF NOT EXISTS silver AUTHORIZATION norgespris;    -- clean tables
CREATE SCHEMA IF NOT EXISTS gold   AUTHORIZATION norgespris;    -- star schema + dashboard tables

-- Print a confirmation:
\echo 'Done: role norgespris, databases norgespris_dev and norgespris_prod with schemas bronze, silver, gold.'
