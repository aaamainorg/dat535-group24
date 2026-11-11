-- =============================================================================
-- One-time PostgreSQL setup for the DAT535 Group 24 pipeline.
--
-- Creates:
--   * the login role  norgespris  (password given on the command line, never stored here)
--   * two databases   norgespris_dev  and  norgespris_prod   (one per environment)
--   * three schemas   bronze, silver, gold   in each database (the Medallion layers)
--
-- Run it ONCE as a PostgreSQL superuser, from the repo root:
--   macOS (Homebrew):  psql -d postgres -v pw='choose-a-password' -f scripts/init_postgres.sql
--   Linux (apt):       sudo -u postgres psql -v pw='choose-a-password' < scripts/init_postgres.sql
--   Docker:            docker exec -i norgespris-postgres psql -U postgres -v pw='choose-a-password' < scripts/init_postgres.sql
-- Then put the same password in .env as NORGESPRIS_PG_PASSWORD.
-- =============================================================================

\set ON_ERROR_STOP on

-- The pipeline logs in as this role. :'pw' inserts the password from "-v pw=..." as a quoted text value.
CREATE ROLE norgespris LOGIN PASSWORD :'pw';

-- One database per environment, owned by the pipeline role (same idea as the landing folders /tmp/norgespris-landing/dev and /prod).
CREATE DATABASE norgespris_dev OWNER norgespris;
CREATE DATABASE norgespris_prod OWNER norgespris;

-- Schemas = Medallion layers. Owned by the pipeline role, so Spark can create and replace tables in them.
\connect norgespris_dev
CREATE SCHEMA IF NOT EXISTS bronze AUTHORIZATION norgespris;
CREATE SCHEMA IF NOT EXISTS silver AUTHORIZATION norgespris;
CREATE SCHEMA IF NOT EXISTS gold   AUTHORIZATION norgespris;

\connect norgespris_prod
CREATE SCHEMA IF NOT EXISTS bronze AUTHORIZATION norgespris;
CREATE SCHEMA IF NOT EXISTS silver AUTHORIZATION norgespris;
CREATE SCHEMA IF NOT EXISTS gold   AUTHORIZATION norgespris;

\echo 'Done: role norgespris, databases norgespris_dev and norgespris_prod with schemas bronze, silver, gold.'
