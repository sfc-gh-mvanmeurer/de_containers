/*
================================================================================
Demo Canvas Data Engineering - Infrastructure Setup
================================================================================
This script creates the foundational Snowflake objects for the demo.
Run this script first with ACCOUNTADMIN or equivalent privileges.
================================================================================
*/

-- ============================================================================
-- STEP 1: Create Demo Database and Schemas
-- ============================================================================

USE ROLE ACCOUNTADMIN;

CREATE DATABASE IF NOT EXISTS DEMO_CANVAS_DB;
USE DATABASE DEMO_CANVAS_DB;

-- Schema for raw/landing data
CREATE SCHEMA IF NOT EXISTS RAW;

-- Schema for transformed/curated data
CREATE SCHEMA IF NOT EXISTS CURATED;

-- Schema for container services and compute
CREATE SCHEMA IF NOT EXISTS COMPUTE;

-- Schema for auditing and monitoring
CREATE SCHEMA IF NOT EXISTS AUDIT;

-- ============================================================================
-- STEP 2: Create Warehouses
-- ============================================================================

-- Warehouse for data ingestion tasks
CREATE WAREHOUSE IF NOT EXISTS DEMO_INGESTION_WH
    WAREHOUSE_SIZE = 'X-SMALL'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE
    RESOURCE_MONITOR = NULL
    COMMENT = 'Warehouse for Canvas data ingestion tasks';

-- Warehouse for data engineering transformations
CREATE WAREHOUSE IF NOT EXISTS DEMO_TRANSFORM_WH
    WAREHOUSE_SIZE = 'SMALL'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE
    COMMENT = 'Warehouse for data engineering transformations';

-- Warehouse for scheduled tasks
CREATE WAREHOUSE IF NOT EXISTS DEMO_TASK_WH
    WAREHOUSE_SIZE = 'X-SMALL'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE
    COMMENT = 'Warehouse for scheduled task execution';

-- ============================================================================
-- STEP 3: Create Roles
-- ============================================================================

CREATE ROLE IF NOT EXISTS DEMO_DATA_ENGINEER;
CREATE ROLE IF NOT EXISTS DEMO_DATA_SCIENTIST;
CREATE ROLE IF NOT EXISTS DEMO_CONTAINER_ADMIN;

-- Grant database access
GRANT USAGE ON DATABASE DEMO_CANVAS_DB TO ROLE DEMO_DATA_ENGINEER;
GRANT USAGE ON DATABASE DEMO_CANVAS_DB TO ROLE DEMO_DATA_SCIENTIST;
GRANT USAGE ON DATABASE DEMO_CANVAS_DB TO ROLE DEMO_CONTAINER_ADMIN;

-- Grant schema access
GRANT ALL ON SCHEMA DEMO_CANVAS_DB.RAW TO ROLE DEMO_DATA_ENGINEER;
GRANT ALL ON SCHEMA DEMO_CANVAS_DB.CURATED TO ROLE DEMO_DATA_ENGINEER;
GRANT ALL ON SCHEMA DEMO_CANVAS_DB.COMPUTE TO ROLE DEMO_CONTAINER_ADMIN;
GRANT ALL ON SCHEMA DEMO_CANVAS_DB.AUDIT TO ROLE DEMO_DATA_ENGINEER;

GRANT USAGE ON SCHEMA DEMO_CANVAS_DB.CURATED TO ROLE DEMO_DATA_SCIENTIST;
GRANT SELECT ON ALL TABLES IN SCHEMA DEMO_CANVAS_DB.CURATED TO ROLE DEMO_DATA_SCIENTIST;
GRANT SELECT ON FUTURE TABLES IN SCHEMA DEMO_CANVAS_DB.CURATED TO ROLE DEMO_DATA_SCIENTIST;

-- Grant warehouse access
GRANT USAGE ON WAREHOUSE DEMO_INGESTION_WH TO ROLE DEMO_DATA_ENGINEER;
GRANT USAGE ON WAREHOUSE DEMO_TRANSFORM_WH TO ROLE DEMO_DATA_ENGINEER;
GRANT USAGE ON WAREHOUSE DEMO_TASK_WH TO ROLE DEMO_DATA_ENGINEER;
GRANT USAGE ON WAREHOUSE DEMO_TRANSFORM_WH TO ROLE DEMO_DATA_SCIENTIST;

-- Role hierarchy
GRANT ROLE DEMO_DATA_ENGINEER TO ROLE ACCOUNTADMIN;
GRANT ROLE DEMO_DATA_SCIENTIST TO ROLE ACCOUNTADMIN;
GRANT ROLE DEMO_CONTAINER_ADMIN TO ROLE ACCOUNTADMIN;

-- ============================================================================
-- STEP 4: Create Internal Stages
-- ============================================================================

USE SCHEMA RAW;

-- Stage for incoming Canvas data files
CREATE STAGE IF NOT EXISTS CANVAS_DATA_STAGE
    DIRECTORY = (ENABLE = TRUE)
    COMMENT = 'Stage for incoming Canvas LMS data files';

-- Stage for container service artifacts
USE SCHEMA COMPUTE;

CREATE STAGE IF NOT EXISTS CONTAINER_ARTIFACTS
    DIRECTORY = (ENABLE = TRUE)
    ENCRYPTION = (TYPE = 'SNOWFLAKE_SSE')
    COMMENT = 'Stage for container service specifications';

-- ============================================================================
-- STEP 5: Create Image Repository for Container Services
-- ============================================================================

CREATE IMAGE REPOSITORY IF NOT EXISTS DEMO_CANVAS_DB.COMPUTE.CANVAS_IMAGES
    COMMENT = 'Image repository for Canvas data engineering containers';

-- Get the repository URL (you'll need this for pushing images)
SHOW IMAGE REPOSITORIES IN SCHEMA DEMO_CANVAS_DB.COMPUTE;

-- ============================================================================
-- STEP 6: Create Compute Pool for Container Services
-- ============================================================================

CREATE COMPUTE POOL IF NOT EXISTS DEMO_CANVAS_POOL
    MIN_NODES = 1
    MAX_NODES = 2
    INSTANCE_FAMILY = CPU_X64_XS
    AUTO_SUSPEND_SECS = 300
    AUTO_RESUME = TRUE
    COMMENT = 'Compute pool for Canvas data engineering workloads';

-- Grant compute pool access
GRANT USAGE ON COMPUTE POOL DEMO_CANVAS_POOL TO ROLE DEMO_CONTAINER_ADMIN;
GRANT MONITOR ON COMPUTE POOL DEMO_CANVAS_POOL TO ROLE DEMO_DATA_ENGINEER;

-- ============================================================================
-- STEP 7: Create Network Rule for External Access (if needed)
-- ============================================================================

CREATE OR REPLACE NETWORK RULE DEMO_EGRESS_RULE
    MODE = EGRESS
    TYPE = HOST_PORT
    VALUE_LIST = ('0.0.0.0:443', '0.0.0.0:80')
    COMMENT = 'Allow outbound HTTPS/HTTP for data ingestion';

CREATE OR REPLACE EXTERNAL ACCESS INTEGRATION DEMO_EXTERNAL_ACCESS
    ALLOWED_NETWORK_RULES = (DEMO_EGRESS_RULE)
    ENABLED = TRUE
    COMMENT = 'External access for Canvas data ingestion';

-- ============================================================================
-- VERIFICATION
-- ============================================================================

-- Verify infrastructure creation
SHOW DATABASES LIKE 'DEMO_CANVAS_DB';
SHOW SCHEMAS IN DATABASE DEMO_CANVAS_DB;
SHOW WAREHOUSES LIKE 'DEMO_%';
SHOW COMPUTE POOLS LIKE 'DEMO_%';
SHOW IMAGE REPOSITORIES IN SCHEMA DEMO_CANVAS_DB.COMPUTE;

SELECT 'Infrastructure setup complete!' AS STATUS;



