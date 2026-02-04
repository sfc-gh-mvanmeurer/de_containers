/*
================================================================================
Demo Canvas Data Engineering - Snowpark Container Services Setup
================================================================================
This script sets up the container service for Python-based data engineering.
================================================================================
*/

USE DATABASE DEMO_CANVAS_DB;
USE SCHEMA COMPUTE;
USE WAREHOUSE DEMO_TRANSFORM_WH;

-- ============================================================================
-- STEP 1: Verify Image Repository
-- ============================================================================

-- Show the image repository URL (needed for Docker push)
SHOW IMAGE REPOSITORIES IN SCHEMA DEMO_CANVAS_DB.COMPUTE;

-- The URL will be in format:
-- <orgname>-<acctname>.registry.snowflakecomputing.com/demo_canvas_db/compute/canvas_images

-- ============================================================================
-- STEP 2: Create Service Specification Stage
-- ============================================================================

-- Stage for YAML specs
CREATE STAGE IF NOT EXISTS CONTAINER_SPECS
    DIRECTORY = (ENABLE = TRUE)
    ENCRYPTION = (TYPE = 'SNOWFLAKE_SSE');

-- ============================================================================
-- STEP 3: Create the Container Service
-- ============================================================================

-- Note: The image must be pushed to the repository first via CI/CD or manually
-- Image name format: <repo_url>/canvas-etl:latest

CREATE SERVICE IF NOT EXISTS CANVAS_ETL_SERVICE
    IN COMPUTE POOL DEMO_CANVAS_POOL
    FROM SPECIFICATION $$
spec:
  containers:
    - name: canvas-etl
      image: /demo_canvas_db/compute/canvas_images/canvas-etl:latest
      env:
        SNOWFLAKE_DATABASE: DEMO_CANVAS_DB
        SNOWFLAKE_SCHEMA_RAW: RAW
        SNOWFLAKE_SCHEMA_CURATED: CURATED
        SNOWFLAKE_WAREHOUSE: DEMO_TRANSFORM_WH
        LOG_LEVEL: INFO
      resources:
        requests:
          cpu: 0.5
          memory: 1Gi
        limits:
          cpu: 2
          memory: 4Gi
      volumeMounts:
        - name: data-volume
          mountPath: /app/data
  volumes:
    - name: data-volume
      source: local
  endpoints:
    - name: etl-endpoint
      port: 8080
      public: false
$$
    MIN_INSTANCES = 1
    MAX_INSTANCES = 2
    EXTERNAL_ACCESS_INTEGRATIONS = (DEMO_EXTERNAL_ACCESS)
    QUERY_WAREHOUSE = DEMO_TRANSFORM_WH
    COMMENT = 'Canvas data engineering ETL service';

-- ============================================================================
-- STEP 4: Create Service Function for ETL Trigger
-- ============================================================================

-- This function allows calling the container service from SQL
CREATE OR REPLACE FUNCTION RUN_CANVAS_ETL(job_type VARCHAR)
RETURNS VARCHAR
SERVICE = CANVAS_ETL_SERVICE
ENDPOINT = 'etl-endpoint'
AS '/run_etl';

-- Function to check ETL status
CREATE OR REPLACE FUNCTION GET_ETL_STATUS()
RETURNS VARIANT
SERVICE = CANVAS_ETL_SERVICE
ENDPOINT = 'etl-endpoint'
AS '/status';

-- Function to run specific transformation
CREATE OR REPLACE FUNCTION RUN_TRANSFORMATION(transformation_name VARCHAR, params VARIANT)
RETURNS VARIANT
SERVICE = CANVAS_ETL_SERVICE
ENDPOINT = 'etl-endpoint'
AS '/transform';

-- ============================================================================
-- STEP 5: Service Management Procedures
-- ============================================================================

-- Procedure to check service health
CREATE OR REPLACE PROCEDURE CHECK_SERVICE_HEALTH()
RETURNS TABLE (
    service_name VARCHAR,
    status VARCHAR,
    container_count NUMBER,
    last_check TIMESTAMP_NTZ
)
LANGUAGE SQL
AS
$$
DECLARE
    result RESULTSET;
BEGIN
    result := (
        SELECT 
            'CANVAS_ETL_SERVICE' AS service_name,
            SYSTEM$GET_SERVICE_STATUS('CANVAS_ETL_SERVICE') AS status,
            1 AS container_count,
            CURRENT_TIMESTAMP() AS last_check
    );
    RETURN TABLE(result);
END;
$$;

-- Procedure to restart service
CREATE OR REPLACE PROCEDURE RESTART_ETL_SERVICE()
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
BEGIN
    ALTER SERVICE CANVAS_ETL_SERVICE SUSPEND;
    ALTER SERVICE CANVAS_ETL_SERVICE RESUME;
    RETURN 'Service restarted successfully';
END;
$$;

-- Procedure to scale service
CREATE OR REPLACE PROCEDURE SCALE_ETL_SERVICE(min_instances INTEGER, max_instances INTEGER)
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
BEGIN
    EXECUTE IMMEDIATE 'ALTER SERVICE CANVAS_ETL_SERVICE SET MIN_INSTANCES = ' || min_instances || ', MAX_INSTANCES = ' || max_instances;
    RETURN 'Service scaled to ' || min_instances || '-' || max_instances || ' instances';
END;
$$;

-- ============================================================================
-- STEP 6: Grant Permissions
-- ============================================================================

GRANT USAGE ON SERVICE CANVAS_ETL_SERVICE TO ROLE DEMO_DATA_ENGINEER;
GRANT USAGE ON FUNCTION RUN_CANVAS_ETL(VARCHAR) TO ROLE DEMO_DATA_ENGINEER;
GRANT USAGE ON FUNCTION GET_ETL_STATUS() TO ROLE DEMO_DATA_ENGINEER;
GRANT USAGE ON FUNCTION RUN_TRANSFORMATION(VARCHAR, VARIANT) TO ROLE DEMO_DATA_ENGINEER;

-- ============================================================================
-- STEP 7: Service Monitoring Views
-- ============================================================================

USE SCHEMA AUDIT;

-- View to monitor service logs
CREATE OR REPLACE VIEW VW_SERVICE_LOGS AS
SELECT 
    TIMESTAMP,
    RECORD['severity']::VARCHAR AS severity,
    VALUE::VARCHAR AS message
FROM TABLE(
    DEMO_CANVAS_DB.COMPUTE.CANVAS_ETL_SERVICE!GET_SERVICE_LOGS('canvas-etl', 100)
);

-- Service metrics view
CREATE OR REPLACE VIEW VW_SERVICE_METRICS AS
SELECT
    start_time,
    end_time,
    credits_used,
    credits_used_cloud_services,
    credits_used_compute
FROM SNOWFLAKE.ACCOUNT_USAGE.SERVERLESS_TASK_HISTORY
WHERE database_name = 'DEMO_CANVAS_DB'
ORDER BY start_time DESC;

-- ============================================================================
-- USAGE EXAMPLES
-- ============================================================================

-- Check service status
-- SELECT SYSTEM$GET_SERVICE_STATUS('DEMO_CANVAS_DB.COMPUTE.CANVAS_ETL_SERVICE');

-- View service logs
-- SELECT * FROM TABLE(DEMO_CANVAS_DB.COMPUTE.CANVAS_ETL_SERVICE!GET_SERVICE_LOGS('canvas-etl', 50));

-- Trigger ETL job
-- SELECT RUN_CANVAS_ETL('FULL_REFRESH');

-- Check ETL status
-- SELECT GET_ETL_STATUS();

-- Describe the service
-- DESCRIBE SERVICE DEMO_CANVAS_DB.COMPUTE.CANVAS_ETL_SERVICE;

-- ============================================================================
-- ALTERNATIVE: Job Service Specification (for batch processing)
-- ============================================================================

-- For one-off batch jobs instead of long-running services:
/*
EXECUTE JOB SERVICE
    IN COMPUTE POOL DEMO_CANVAS_POOL
    NAME = CANVAS_ETL_BATCH_JOB
    FROM SPECIFICATION $$
spec:
  containers:
    - name: canvas-etl-batch
      image: /demo_canvas_db/compute/canvas_images/canvas-etl:latest
      env:
        JOB_TYPE: FULL_REFRESH
        SNOWFLAKE_DATABASE: DEMO_CANVAS_DB
      command:
        - python
        - /app/main.py
        - --job-type
        - full_refresh
$$;
*/

SELECT 'Container service setup complete!' AS STATUS;



