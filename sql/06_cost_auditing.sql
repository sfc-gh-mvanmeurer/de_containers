/*
================================================================================
Demo Canvas Data Engineering - Cost Auditing & Monitoring
================================================================================
This script creates views and procedures for monitoring compute costs
and attributing them to specific workflow components.
================================================================================
*/

USE DATABASE DEMO_CANVAS_DB;
USE SCHEMA AUDIT;
USE WAREHOUSE DEMO_TASK_WH;

-- ============================================================================
-- SECTION 1: Cost Tracking Tables
-- ============================================================================

-- Table to store cost attribution rules
CREATE OR REPLACE TABLE COST_ATTRIBUTION_RULES (
    rule_id         NUMBER AUTOINCREMENT PRIMARY KEY,
    resource_type   VARCHAR(50),   -- WAREHOUSE, CONTAINER, TASK, STORAGE
    resource_name   VARCHAR(200),
    cost_center     VARCHAR(100),
    department      VARCHAR(100),
    project         VARCHAR(100),
    created_at      TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    updated_at      TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Insert default cost attribution rules
INSERT INTO COST_ATTRIBUTION_RULES (resource_type, resource_name, cost_center, department, project)
VALUES 
    ('WAREHOUSE', 'DEMO_INGESTION_WH', 'DATA_PLATFORM', 'IT', 'CANVAS_ETL'),
    ('WAREHOUSE', 'DEMO_TRANSFORM_WH', 'DATA_PLATFORM', 'IT', 'CANVAS_ETL'),
    ('WAREHOUSE', 'DEMO_TASK_WH', 'DATA_PLATFORM', 'IT', 'CANVAS_ETL'),
    ('CONTAINER', 'CANVAS_ETL_SERVICE', 'DATA_PLATFORM', 'IT', 'CANVAS_ETL'),
    ('COMPUTE_POOL', 'DEMO_CANVAS_POOL', 'DATA_PLATFORM', 'IT', 'CANVAS_ETL');

-- Daily cost snapshot table
CREATE OR REPLACE TABLE DAILY_COST_SNAPSHOT (
    snapshot_id     NUMBER AUTOINCREMENT PRIMARY KEY,
    snapshot_date   DATE,
    resource_type   VARCHAR(50),
    resource_name   VARCHAR(200),
    credits_used    DECIMAL(20,10),
    estimated_cost_usd DECIMAL(15,2),
    cost_center     VARCHAR(100),
    department      VARCHAR(100),
    project         VARCHAR(100),
    metadata        VARIANT,
    created_at      TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- ============================================================================
-- SECTION 2: Warehouse Cost Views
-- ============================================================================

-- View: Warehouse usage by hour (last 7 days)
CREATE OR REPLACE VIEW VW_WAREHOUSE_USAGE_HOURLY AS
SELECT 
    DATE_TRUNC('hour', start_time) AS usage_hour,
    warehouse_name,
    SUM(credits_used) AS total_credits,
    SUM(credits_used) * 3.00 AS estimated_cost_usd,  -- Adjust rate as needed
    COUNT(*) AS query_count,
    AVG(DATEDIFF('second', start_time, end_time)) AS avg_query_duration_sec
FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
WHERE start_time >= DATEADD('day', -7, CURRENT_DATE())
    AND warehouse_name LIKE 'DEMO_%'
GROUP BY 1, 2
ORDER BY 1 DESC, 2;

-- View: Daily warehouse costs with attribution
CREATE OR REPLACE VIEW VW_WAREHOUSE_DAILY_COSTS AS
SELECT 
    DATE_TRUNC('day', wmh.start_time) AS usage_date,
    wmh.warehouse_name,
    car.cost_center,
    car.department,
    car.project,
    SUM(wmh.credits_used) AS total_credits,
    SUM(wmh.credits_used) * 3.00 AS estimated_cost_usd,
    ROUND(SUM(wmh.credits_used) / NULLIF(SUM(SUM(wmh.credits_used)) OVER (PARTITION BY DATE_TRUNC('day', wmh.start_time)), 0) * 100, 2) AS pct_of_daily_spend
FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY wmh
LEFT JOIN COST_ATTRIBUTION_RULES car 
    ON car.resource_type = 'WAREHOUSE' 
    AND car.resource_name = wmh.warehouse_name
WHERE wmh.start_time >= DATEADD('day', -30, CURRENT_DATE())
    AND wmh.warehouse_name LIKE 'DEMO_%'
GROUP BY 1, 2, 3, 4, 5
ORDER BY 1 DESC, total_credits DESC;

-- View: Warehouse cost trends (monthly)
CREATE OR REPLACE VIEW VW_WAREHOUSE_MONTHLY_TRENDS AS
SELECT 
    DATE_TRUNC('month', start_time) AS usage_month,
    warehouse_name,
    SUM(credits_used) AS total_credits,
    SUM(credits_used) * 3.00 AS estimated_cost_usd,
    LAG(SUM(credits_used)) OVER (PARTITION BY warehouse_name ORDER BY DATE_TRUNC('month', start_time)) AS prev_month_credits,
    ROUND((SUM(credits_used) - LAG(SUM(credits_used)) OVER (PARTITION BY warehouse_name ORDER BY DATE_TRUNC('month', start_time))) 
        / NULLIF(LAG(SUM(credits_used)) OVER (PARTITION BY warehouse_name ORDER BY DATE_TRUNC('month', start_time)), 0) * 100, 2) AS mom_change_pct
FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
WHERE start_time >= DATEADD('month', -12, CURRENT_DATE())
    AND warehouse_name LIKE 'DEMO_%'
GROUP BY 1, 2
ORDER BY 1 DESC, 2;

-- ============================================================================
-- SECTION 3: Container Service Cost Views
-- ============================================================================

-- View: Compute Pool usage
CREATE OR REPLACE VIEW VW_COMPUTE_POOL_USAGE AS
SELECT 
    DATE_TRUNC('hour', start_time) AS usage_hour,
    compute_pool_name,
    SUM(credits_used) AS total_credits,
    SUM(credits_used) * 3.00 AS estimated_cost_usd,
    AVG(active_node_count) AS avg_active_nodes,
    MAX(active_node_count) AS max_active_nodes
FROM SNOWFLAKE.ACCOUNT_USAGE.SNOWPARK_CONTAINER_SERVICES_HISTORY
WHERE start_time >= DATEADD('day', -30, CURRENT_DATE())
    AND compute_pool_name LIKE 'DEMO_%'
GROUP BY 1, 2
ORDER BY 1 DESC;

-- View: Container service daily costs
CREATE OR REPLACE VIEW VW_CONTAINER_DAILY_COSTS AS
SELECT 
    DATE_TRUNC('day', spcs.start_time) AS usage_date,
    spcs.compute_pool_name,
    car.cost_center,
    car.department,
    car.project,
    SUM(spcs.credits_used) AS total_credits,
    SUM(spcs.credits_used) * 3.00 AS estimated_cost_usd,
    SUM(spcs.credits_used_compute) AS compute_credits,
    SUM(spcs.credits_used_cloud_services) AS cloud_services_credits
FROM SNOWFLAKE.ACCOUNT_USAGE.SNOWPARK_CONTAINER_SERVICES_HISTORY spcs
LEFT JOIN COST_ATTRIBUTION_RULES car 
    ON car.resource_type = 'COMPUTE_POOL' 
    AND car.resource_name = spcs.compute_pool_name
WHERE spcs.start_time >= DATEADD('day', -30, CURRENT_DATE())
    AND spcs.compute_pool_name LIKE 'DEMO_%'
GROUP BY 1, 2, 3, 4, 5
ORDER BY 1 DESC;

-- ============================================================================
-- SECTION 4: Task Cost Views
-- ============================================================================

-- View: Task execution costs
CREATE OR REPLACE VIEW VW_TASK_EXECUTION_COSTS AS
SELECT 
    DATE_TRUNC('day', query_start_time) AS execution_date,
    database_name,
    schema_name,
    name AS task_name,
    state,
    COUNT(*) AS execution_count,
    SUM(credits_used) AS total_credits,
    SUM(credits_used) * 3.00 AS estimated_cost_usd,
    AVG(DATEDIFF('second', query_start_time, completed_time)) AS avg_duration_sec,
    SUM(CASE WHEN state = 'FAILED' THEN 1 ELSE 0 END) AS failed_count
FROM SNOWFLAKE.ACCOUNT_USAGE.TASK_HISTORY
WHERE query_start_time >= DATEADD('day', -30, CURRENT_DATE())
    AND database_name = 'DEMO_CANVAS_DB'
GROUP BY 1, 2, 3, 4, 5
ORDER BY 1 DESC, total_credits DESC;

-- View: Task failure analysis
CREATE OR REPLACE VIEW VW_TASK_FAILURES AS
SELECT 
    scheduled_time,
    name AS task_name,
    schema_name,
    state,
    error_code,
    error_message,
    DATEDIFF('second', query_start_time, completed_time) AS duration_sec
FROM SNOWFLAKE.ACCOUNT_USAGE.TASK_HISTORY
WHERE database_name = 'DEMO_CANVAS_DB'
    AND state = 'FAILED'
    AND scheduled_time >= DATEADD('day', -7, CURRENT_DATE())
ORDER BY scheduled_time DESC;

-- ============================================================================
-- SECTION 5: Storage Cost Views
-- ============================================================================

-- View: Database storage costs
CREATE OR REPLACE VIEW VW_STORAGE_COSTS AS
SELECT 
    usage_date,
    database_name,
    ROUND(average_database_bytes / POWER(1024, 3), 4) AS storage_gb,
    ROUND(average_failsafe_bytes / POWER(1024, 3), 4) AS failsafe_gb,
    ROUND((average_database_bytes + average_failsafe_bytes) / POWER(1024, 4), 4) AS total_tb,
    ROUND((average_database_bytes + average_failsafe_bytes) / POWER(1024, 4) * 23, 2) AS estimated_monthly_cost_usd  -- $23/TB/month
FROM SNOWFLAKE.ACCOUNT_USAGE.DATABASE_STORAGE_USAGE_HISTORY
WHERE database_name = 'DEMO_CANVAS_DB'
    AND usage_date >= DATEADD('day', -30, CURRENT_DATE())
ORDER BY usage_date DESC;

-- View: Table-level storage
CREATE OR REPLACE VIEW VW_TABLE_STORAGE AS
SELECT 
    table_catalog AS database_name,
    table_schema,
    table_name,
    ROUND(active_bytes / POWER(1024, 2), 2) AS active_mb,
    ROUND(time_travel_bytes / POWER(1024, 2), 2) AS time_travel_mb,
    ROUND(failsafe_bytes / POWER(1024, 2), 2) AS failsafe_mb,
    ROUND((active_bytes + time_travel_bytes + failsafe_bytes) / POWER(1024, 2), 2) AS total_mb,
    row_count,
    ROUND(active_bytes / NULLIF(row_count, 0), 2) AS bytes_per_row
FROM SNOWFLAKE.ACCOUNT_USAGE.TABLE_STORAGE_METRICS
WHERE table_catalog = 'DEMO_CANVAS_DB'
    AND deleted IS NULL
ORDER BY active_bytes DESC;

-- ============================================================================
-- SECTION 6: Combined Cost Dashboard View
-- ============================================================================

-- View: Executive cost summary
CREATE OR REPLACE VIEW VW_COST_DASHBOARD AS
WITH warehouse_costs AS (
    SELECT 
        'WAREHOUSE' AS resource_type,
        warehouse_name AS resource_name,
        SUM(credits_used) AS total_credits
    FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
    WHERE start_time >= DATEADD('day', -30, CURRENT_DATE())
        AND warehouse_name LIKE 'DEMO_%'
    GROUP BY 2
),
container_costs AS (
    SELECT 
        'CONTAINER' AS resource_type,
        compute_pool_name AS resource_name,
        SUM(credits_used) AS total_credits
    FROM SNOWFLAKE.ACCOUNT_USAGE.SNOWPARK_CONTAINER_SERVICES_HISTORY
    WHERE start_time >= DATEADD('day', -30, CURRENT_DATE())
        AND compute_pool_name LIKE 'DEMO_%'
    GROUP BY 2
),
task_costs AS (
    SELECT 
        'TASK' AS resource_type,
        name AS resource_name,
        SUM(credits_used) AS total_credits
    FROM SNOWFLAKE.ACCOUNT_USAGE.TASK_HISTORY
    WHERE query_start_time >= DATEADD('day', -30, CURRENT_DATE())
        AND database_name = 'DEMO_CANVAS_DB'
    GROUP BY 2
),
all_costs AS (
    SELECT * FROM warehouse_costs
    UNION ALL
    SELECT * FROM container_costs
    UNION ALL
    SELECT * FROM task_costs
)
SELECT 
    ac.resource_type,
    ac.resource_name,
    car.cost_center,
    car.department,
    car.project,
    ac.total_credits,
    ROUND(ac.total_credits * 3.00, 2) AS estimated_cost_usd,
    ROUND(ac.total_credits / NULLIF(SUM(ac.total_credits) OVER (), 0) * 100, 2) AS pct_of_total
FROM all_costs ac
LEFT JOIN COST_ATTRIBUTION_RULES car 
    ON car.resource_name = ac.resource_name
ORDER BY total_credits DESC;

-- ============================================================================
-- SECTION 7: Cost Alerting Procedures
-- ============================================================================

-- Table for cost alerts
CREATE OR REPLACE TABLE COST_ALERTS (
    alert_id        NUMBER AUTOINCREMENT PRIMARY KEY,
    alert_type      VARCHAR(50),
    resource_type   VARCHAR(50),
    resource_name   VARCHAR(200),
    threshold_value DECIMAL(15,2),
    current_value   DECIMAL(15,2),
    alert_message   VARCHAR(1000),
    alert_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    acknowledged    BOOLEAN DEFAULT FALSE,
    acknowledged_by VARCHAR(100),
    acknowledged_at TIMESTAMP_NTZ
);

-- Procedure to check cost thresholds
CREATE OR REPLACE PROCEDURE CHECK_COST_THRESHOLDS()
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
    daily_threshold DECIMAL(15,2) := 50.00;
    hourly_threshold DECIMAL(15,2) := 10.00;
BEGIN
    -- Check daily warehouse costs
    INSERT INTO COST_ALERTS (alert_type, resource_type, resource_name, threshold_value, current_value, alert_message)
    SELECT 
        'DAILY_THRESHOLD_EXCEEDED',
        'WAREHOUSE',
        warehouse_name,
        :daily_threshold,
        SUM(credits_used) * 3.00,
        'Daily cost threshold exceeded for warehouse ' || warehouse_name || ': $' || ROUND(SUM(credits_used) * 3.00, 2)
    FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
    WHERE start_time >= CURRENT_DATE()
        AND warehouse_name LIKE 'DEMO_%'
    GROUP BY warehouse_name
    HAVING SUM(credits_used) * 3.00 > :daily_threshold;
    
    -- Check hourly spike
    INSERT INTO COST_ALERTS (alert_type, resource_type, resource_name, threshold_value, current_value, alert_message)
    SELECT 
        'HOURLY_SPIKE',
        'WAREHOUSE',
        warehouse_name,
        :hourly_threshold,
        SUM(credits_used) * 3.00,
        'Unusual hourly cost spike for warehouse ' || warehouse_name || ': $' || ROUND(SUM(credits_used) * 3.00, 2)
    FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
    WHERE start_time >= DATEADD('hour', -1, CURRENT_TIMESTAMP())
        AND warehouse_name LIKE 'DEMO_%'
    GROUP BY warehouse_name
    HAVING SUM(credits_used) * 3.00 > :hourly_threshold;
    
    RETURN 'Cost threshold check completed';
END;
$$;

-- Schedule cost monitoring
CREATE OR REPLACE TASK TASK_COST_MONITORING
    WAREHOUSE = DEMO_TASK_WH
    SCHEDULE = 'USING CRON 0 * * * * America/New_York'  -- Every hour
    COMMENT = 'Monitors costs and generates alerts when thresholds are exceeded'
AS
    CALL CHECK_COST_THRESHOLDS();

-- ============================================================================
-- SECTION 8: Daily Cost Snapshot Procedure
-- ============================================================================

CREATE OR REPLACE PROCEDURE CAPTURE_DAILY_COST_SNAPSHOT()
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
BEGIN
    -- Capture warehouse costs
    INSERT INTO DAILY_COST_SNAPSHOT (snapshot_date, resource_type, resource_name, credits_used, estimated_cost_usd, cost_center, department, project)
    SELECT 
        CURRENT_DATE() - 1,
        'WAREHOUSE',
        wmh.warehouse_name,
        SUM(wmh.credits_used),
        SUM(wmh.credits_used) * 3.00,
        car.cost_center,
        car.department,
        car.project
    FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY wmh
    LEFT JOIN COST_ATTRIBUTION_RULES car ON car.resource_name = wmh.warehouse_name
    WHERE DATE_TRUNC('day', wmh.start_time) = CURRENT_DATE() - 1
        AND wmh.warehouse_name LIKE 'DEMO_%'
    GROUP BY wmh.warehouse_name, car.cost_center, car.department, car.project;
    
    -- Capture container costs
    INSERT INTO DAILY_COST_SNAPSHOT (snapshot_date, resource_type, resource_name, credits_used, estimated_cost_usd, cost_center, department, project)
    SELECT 
        CURRENT_DATE() - 1,
        'CONTAINER',
        spcs.compute_pool_name,
        SUM(spcs.credits_used),
        SUM(spcs.credits_used) * 3.00,
        car.cost_center,
        car.department,
        car.project
    FROM SNOWFLAKE.ACCOUNT_USAGE.SNOWPARK_CONTAINER_SERVICES_HISTORY spcs
    LEFT JOIN COST_ATTRIBUTION_RULES car ON car.resource_name = spcs.compute_pool_name
    WHERE DATE_TRUNC('day', spcs.start_time) = CURRENT_DATE() - 1
        AND spcs.compute_pool_name LIKE 'DEMO_%'
    GROUP BY spcs.compute_pool_name, car.cost_center, car.department, car.project;
    
    RETURN 'Daily cost snapshot captured for ' || (CURRENT_DATE() - 1)::VARCHAR;
END;
$$;

-- Schedule daily snapshot
CREATE OR REPLACE TASK TASK_DAILY_COST_SNAPSHOT
    WAREHOUSE = DEMO_TASK_WH
    SCHEDULE = 'USING CRON 0 1 * * * America/New_York'  -- Daily at 1 AM
    COMMENT = 'Captures daily cost snapshot for historical analysis'
AS
    CALL CAPTURE_DAILY_COST_SNAPSHOT();

-- ============================================================================
-- SECTION 9: Cost Reporting Queries
-- ============================================================================

-- Quick cost summary query (run this for demos)
/*
SELECT 
    resource_type,
    resource_name,
    ROUND(SUM(credits_used), 4) AS total_credits,
    ROUND(SUM(estimated_cost_usd), 2) AS total_cost_usd
FROM DAILY_COST_SNAPSHOT
WHERE snapshot_date >= DATEADD('day', -7, CURRENT_DATE())
GROUP BY 1, 2
ORDER BY total_cost_usd DESC;
*/

-- Cost by project query
/*
SELECT 
    project,
    department,
    ROUND(SUM(credits_used), 4) AS total_credits,
    ROUND(SUM(estimated_cost_usd), 2) AS total_cost_usd,
    COUNT(DISTINCT resource_name) AS resource_count
FROM DAILY_COST_SNAPSHOT
WHERE snapshot_date >= DATEADD('day', -30, CURRENT_DATE())
GROUP BY 1, 2
ORDER BY total_cost_usd DESC;
*/

-- ============================================================================
-- ENABLE COST MONITORING TASKS
-- ============================================================================

-- ALTER TASK TASK_DAILY_COST_SNAPSHOT RESUME;
-- ALTER TASK TASK_COST_MONITORING RESUME;

SELECT 'Cost auditing setup complete!' AS STATUS;



