# FGCU Canvas Data Engineering Demo Walkthrough

> Step-by-step guide to demonstrate the complete data engineering pipeline in Snowflake Snowsight

## 🎬 Demo Script

### Prerequisites Checklist

- [ ] Snowflake account with ACCOUNTADMIN access
- [ ] GitHub repository at `https://github.com/sfc-gh-mvanmeurer/fgcu_de_containers`
- [ ] GitHub Secrets configured (see below)

---

## Part 1: Infrastructure Setup (5 minutes)

### Open Snowsight SQL Worksheet

1. Navigate to **Snowsight** → **Worksheets** → **+ Worksheet**
2. Name the worksheet: `FGCU Canvas Demo Setup`

### Run Infrastructure Script

```sql
-- Copy and paste from: sql/01_setup_infrastructure.sql
-- This creates:
--   ✓ FGCU_CANVAS_DEMO database
--   ✓ RAW, CURATED, COMPUTE, AUDIT schemas
--   ✓ Warehouses (FGCU_INGESTION_WH, FGCU_TRANSFORM_WH, FGCU_TASK_WH)
--   ✓ Image repository for containers
--   ✓ Compute pool for SPCS
```

### Verify Setup

```sql
SHOW DATABASES LIKE 'FGCU_CANVAS_DEMO';
SHOW SCHEMAS IN DATABASE FGCU_CANVAS_DEMO;
SHOW WAREHOUSES LIKE 'FGCU_%';
SHOW COMPUTE POOLS LIKE 'FGCU_%';

-- Get your image repository URL (needed for CI/CD)
SHOW IMAGE REPOSITORIES IN SCHEMA FGCU_CANVAS_DEMO.COMPUTE;
```

**💡 Demo Talking Point:** "We've created a complete data platform infrastructure including separate schemas for raw landing data, curated analytics, and container compute resources."

---

## Part 2: Canvas Data Model (3 minutes)

### Run Data Model Script

```sql
-- Copy and paste from: sql/02_canvas_data_model.sql
-- This creates the Canvas LMS data model:
--   ✓ RAW layer tables (VARIANT storage)
--   ✓ Dimension tables (DIM_STUDENTS, DIM_COURSES, DIM_ASSIGNMENTS)
--   ✓ Fact tables (FACT_ENROLLMENTS, FACT_SUBMISSIONS, FACT_ACTIVITY_LOGS)
--   ✓ Aggregation tables for analytics
--   ✓ Streams for change data capture
```

### Explore the Data Model

```sql
-- View all tables
SELECT table_schema, table_name, row_count
FROM FGCU_CANVAS_DEMO.INFORMATION_SCHEMA.TABLES
WHERE table_type = 'BASE TABLE'
ORDER BY table_schema, table_name;

-- View streams
SHOW STREAMS IN DATABASE FGCU_CANVAS_DEMO;
```

**💡 Demo Talking Point:** "This medallion architecture separates raw ingestion from curated analytics, with streams enabling efficient change data capture."

---

## Part 3: Synthetic Data Generation (5 minutes)

### Deploy Data Generator

```sql
-- Copy and paste from: sql/03_dummy_data_generator.sql
-- This creates stored procedures for generating realistic Canvas data
```

### Generate Sample Data

```sql
USE DATABASE FGCU_CANVAS_DEMO;
USE SCHEMA RAW;
USE WAREHOUSE FGCU_TASK_WH;

-- Generate a complete dataset
CALL GENERATE_COMPLETE_CANVAS_DATASET(
    100,   -- 100 students
    20,    -- 20 courses
    5,     -- 5 enrollments per student
    15,    -- 15 assignments per course
    25     -- 25 activity logs per enrollment
);

-- Or generate individually:
-- CALL GENERATE_DUMMY_STUDENTS(50);
-- CALL GENERATE_DUMMY_COURSES(10);
```

### Verify Generated Data

```sql
-- Check record counts
SELECT 'RAW_STUDENTS' AS table_name, COUNT(*) AS record_count FROM RAW_STUDENTS
UNION ALL
SELECT 'RAW_COURSES', COUNT(*) FROM RAW_COURSES
UNION ALL
SELECT 'RAW_ENROLLMENTS', COUNT(*) FROM RAW_ENROLLMENTS
UNION ALL
SELECT 'RAW_ASSIGNMENTS', COUNT(*) FROM RAW_ASSIGNMENTS
UNION ALL
SELECT 'RAW_SUBMISSIONS', COUNT(*) FROM RAW_SUBMISSIONS
UNION ALL
SELECT 'RAW_ACTIVITY_LOGS', COUNT(*) FROM RAW_ACTIVITY_LOGS;

-- Preview student data
SELECT 
    payload:student_id::VARCHAR AS student_id,
    payload:first_name::VARCHAR AS first_name,
    payload:last_name::VARCHAR AS last_name,
    payload:major::VARCHAR AS major,
    payload:gpa::NUMBER(3,2) AS gpa
FROM RAW_STUDENTS
LIMIT 10;
```

**💡 Demo Talking Point:** "Using Python UDFs with Faker, we generate realistic student data that mimics actual Canvas LMS exports."

---

## Part 4: Scheduled Tasks (3 minutes)

### Deploy Tasks

```sql
-- Copy and paste from: sql/04_scheduled_task.sql
-- This creates:
--   ✓ TASK_GENERATE_DUMMY_DATA (simulates incoming data)
--   ✓ TASK_PROCESS_RAW_STUDENTS (stream-based processing)
--   ✓ TASK_TRIGGER_CONTAINER_ETL (container orchestration)
--   ✓ TASK_REFRESH_AGGREGATIONS (analytics updates)
```

### View Task DAG

```sql
-- Show all tasks
SHOW TASKS IN SCHEMA FGCU_CANVAS_DEMO.RAW;

-- View task dependencies
SELECT name, warehouse, schedule, predecessors, state
FROM TABLE(INFORMATION_SCHEMA.TASK_DEPENDENTS(
    TASK_NAME => 'FGCU_CANVAS_DEMO.RAW.TASK_GENERATE_DUMMY_DATA',
    RECURSIVE => TRUE
));
```

### Enable Tasks (Optional for Demo)

```sql
-- Start the task pipeline
ALTER TASK TASK_GENERATE_DUMMY_DATA RESUME;

-- Check task history
SELECT *
FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY())
WHERE DATABASE_NAME = 'FGCU_CANVAS_DEMO'
ORDER BY SCHEDULED_TIME DESC
LIMIT 10;
```

**💡 Demo Talking Point:** "Snowflake Tasks create a DAG-based orchestration layer, automatically processing new data when streams detect changes."

---

## Part 5: Container Services (7 minutes)

### Get Image Repository URL

```sql
SHOW IMAGE REPOSITORIES IN SCHEMA FGCU_CANVAS_DEMO.COMPUTE;
-- Note the repository_url value
```

### Build and Push Container (Local Terminal)

```bash
cd container

# Build the image
docker build -t canvas-etl:latest .

# Tag for Snowflake registry (replace with your URL)
REPO_URL="<your-account>.registry.snowflakecomputing.com/fgcu_canvas_demo/compute/canvas_images"
docker tag canvas-etl:latest $REPO_URL/canvas-etl:latest

# Login to Snowflake registry
docker login $REPO_URL -u <your-username>

# Push image
docker push $REPO_URL/canvas-etl:latest
```

### Deploy Container Service

```sql
-- Copy and paste from: sql/05_container_service.sql
-- Update the image path if needed

-- Verify service
SELECT SYSTEM$GET_SERVICE_STATUS('FGCU_CANVAS_DEMO.COMPUTE.CANVAS_ETL_SERVICE');

-- View service logs
SELECT * FROM TABLE(
    FGCU_CANVAS_DEMO.COMPUTE.CANVAS_ETL_SERVICE!GET_SERVICE_LOGS('canvas-etl', 50)
);
```

### Call Service Functions

```sql
-- Run ETL job via service function
SELECT FGCU_CANVAS_DEMO.COMPUTE.RUN_CANVAS_ETL('FULL_REFRESH');

-- Check ETL status
SELECT FGCU_CANVAS_DEMO.COMPUTE.GET_ETL_STATUS();
```

**💡 Demo Talking Point:** "Snowpark Container Services lets us run our Python ETL as a managed service, with automatic scaling and direct Snowflake integration."

---

## Part 6: Cost Auditing (5 minutes)

### Deploy Cost Monitoring

```sql
-- Copy and paste from: sql/06_cost_auditing.sql
```

### View Cost Dashboard

```sql
USE SCHEMA FGCU_CANVAS_DEMO.AUDIT;

-- Executive cost summary (last 30 days)
SELECT * FROM VW_COST_DASHBOARD;

-- Daily warehouse costs with attribution
SELECT * FROM VW_WAREHOUSE_DAILY_COSTS
ORDER BY usage_date DESC, total_credits DESC;

-- Container service costs
SELECT * FROM VW_CONTAINER_DAILY_COSTS
ORDER BY usage_date DESC;

-- Task execution costs
SELECT * FROM VW_TASK_EXECUTION_COSTS
ORDER BY execution_date DESC;
```

### Set Up Cost Alerts

```sql
-- Run cost threshold check
CALL CHECK_COST_THRESHOLDS();

-- View any alerts
SELECT * FROM COST_ALERTS
WHERE acknowledged = FALSE
ORDER BY alert_timestamp DESC;

-- Enable automated cost monitoring
ALTER TASK TASK_COST_MONITORING RESUME;
```

### Storage Analysis

```sql
-- Database storage costs
SELECT * FROM VW_STORAGE_COSTS
ORDER BY usage_date DESC;

-- Table-level storage breakdown
SELECT * FROM VW_TABLE_STORAGE
ORDER BY total_mb DESC
LIMIT 20;
```

**💡 Demo Talking Point:** "With views on ACCOUNT_USAGE, we get complete visibility into compute and storage costs, with attribution to specific workflows and cost centers."

---

## Part 7: CI/CD with GitHub (5 minutes)

### Configure GitHub Repository

1. Push code to `https://github.com/sfc-gh-mvanmeurer/fgcu_de_containers`

2. Add Repository Secrets (Settings → Secrets → Actions):

| Secret | Description |
|--------|-------------|
| `SNOWFLAKE_ACCOUNT` | Your account identifier (e.g., `xy12345.us-east-1`) |
| `SNOWFLAKE_USER` | Service account username |
| `SNOWFLAKE_PASSWORD` | Service account password |
| `SNOWFLAKE_ROLE` | Role with container registry access |

3. View the workflow file: `.github/workflows/deploy.yml`

### Trigger Deployment

```bash
# Make a change to container code
echo "# Updated $(date)" >> container/app/__init__.py

# Commit and push
git add .
git commit -m "Trigger CI/CD deployment"
git push origin main
```

### Monitor Pipeline

- Go to GitHub → Actions → Watch the pipeline run
- Stages: Test → Build → Push → Deploy → Validate

### Verify in Snowflake

```sql
-- Check for new image
SHOW IMAGES IN IMAGE REPOSITORY FGCU_CANVAS_DEMO.COMPUTE.CANVAS_IMAGES;

-- View deployment log
SELECT *
FROM FGCU_CANVAS_DEMO.AUDIT.ETL_RUN_LOG
WHERE run_type = 'DEPLOYMENT'
ORDER BY started_at DESC
LIMIT 5;

-- Check service is updated
DESCRIBE SERVICE FGCU_CANVAS_DEMO.COMPUTE.CANVAS_ETL_SERVICE;
```

**💡 Demo Talking Point:** "GitHub Actions automatically builds, tests, and deploys our container to Snowflake's registry, enabling true CI/CD for data engineering."

---

## Part 8: Analytics Demo (3 minutes)

### View Processed Data

```sql
USE SCHEMA FGCU_CANVAS_DEMO.CURATED;

-- Student dimension
SELECT * FROM DIM_STUDENTS LIMIT 10;

-- Course dimension  
SELECT * FROM DIM_COURSES LIMIT 10;

-- Enrollment facts
SELECT 
    s.first_name || ' ' || s.last_name AS student_name,
    c.course_code,
    c.course_name,
    e.enrollment_state,
    e.final_grade,
    e.final_score
FROM FACT_ENROLLMENTS e
JOIN DIM_STUDENTS s ON e.student_id = s.student_id
JOIN DIM_COURSES c ON e.course_id = c.course_id
LIMIT 20;
```

### Analytics Queries

```sql
-- Student performance by major
SELECT 
    major,
    COUNT(*) AS student_count,
    ROUND(AVG(gpa), 2) AS avg_gpa,
    ROUND(AVG(avg_score), 1) AS avg_course_score
FROM DIM_STUDENTS s
LEFT JOIN AGG_STUDENT_COURSE_PERFORMANCE p ON s.student_id = p.student_id
GROUP BY major
ORDER BY avg_gpa DESC;

-- Course analytics
SELECT 
    c.course_code,
    c.course_name,
    a.total_enrolled,
    a.active_students,
    a.avg_class_score,
    a.completion_rate,
    a.at_risk_students
FROM AGG_COURSE_ANALYTICS a
JOIN DIM_COURSES c ON a.course_id = c.course_id
ORDER BY a.total_enrolled DESC;

-- At-risk students
SELECT 
    s.student_id,
    s.first_name || ' ' || s.last_name AS student_name,
    s.major,
    s.gpa,
    p.avg_score,
    p.late_submissions,
    p.missing_submissions
FROM DIM_STUDENTS s
JOIN AGG_STUDENT_COURSE_PERFORMANCE p ON s.student_id = p.student_id
WHERE p.avg_score < 70 OR p.late_submissions > 3 OR p.missing_submissions > 2
ORDER BY p.avg_score ASC;
```

**💡 Demo Talking Point:** "The curated layer provides clean, analytics-ready data for downstream reporting and machine learning applications."

---

## Cleanup (Optional)

```sql
-- Suspend tasks
ALTER TASK FGCU_CANVAS_DEMO.RAW.TASK_GENERATE_DUMMY_DATA SUSPEND;
ALTER TASK FGCU_CANVAS_DEMO.AUDIT.TASK_COST_MONITORING SUSPEND;

-- Suspend service
ALTER SERVICE FGCU_CANVAS_DEMO.COMPUTE.CANVAS_ETL_SERVICE SUSPEND;

-- Suspend compute pool
ALTER COMPUTE POOL FGCU_CANVAS_POOL SUSPEND;

-- Full cleanup (removes everything)
-- DROP DATABASE FGCU_CANVAS_DEMO;
-- DROP COMPUTE POOL FGCU_CANVAS_POOL;
-- DROP WAREHOUSE FGCU_INGESTION_WH;
-- DROP WAREHOUSE FGCU_TRANSFORM_WH;
-- DROP WAREHOUSE FGCU_TASK_WH;
```

---

## Key Takeaways

1. **Snowpark Container Services** enables Python-based data engineering with enterprise scalability
2. **Tasks + Streams** provide efficient, event-driven data processing
3. **Cost Attribution** through ACCOUNT_USAGE views enables FinOps best practices
4. **CI/CD Integration** with GitHub Actions creates a modern DevOps workflow
5. **Canvas Data Model** demonstrates real-world higher education analytics use case

---

## Resources

- [Snowpark Container Services Docs](https://docs.snowflake.com/en/developer-guide/snowpark-container-services/overview)
- [Snowflake Tasks](https://docs.snowflake.com/en/user-guide/tasks-intro)
- [Account Usage Views](https://docs.snowflake.com/en/sql-reference/account-usage)
- [GitHub Actions for Snowflake](https://github.com/Snowflake-Labs/snowflake-cli)



