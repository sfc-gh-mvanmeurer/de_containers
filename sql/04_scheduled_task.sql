/*
================================================================================
FGCU Canvas Data Engineering - Scheduled Tasks
================================================================================
This script creates tasks that orchestrate the data pipeline:
1. Generate dummy data periodically
2. Trigger the container-based ETL process
3. Refresh aggregations
================================================================================
*/

USE DATABASE FGCU_CANVAS_DEMO;
USE SCHEMA RAW;
USE WAREHOUSE FGCU_TASK_WH;

-- ============================================================================
-- TASK 1: Generate New Dummy Data (Simulates incoming Canvas data)
-- ============================================================================

-- This task simulates new data arriving from Canvas LMS
-- In production, this would be replaced by actual API calls or file ingestion

CREATE OR REPLACE TASK TASK_GENERATE_DUMMY_DATA
    WAREHOUSE = FGCU_TASK_WH
    SCHEDULE = 'USING CRON 0 */2 * * * America/New_York'  -- Every 2 hours
    COMMENT = 'Generates synthetic Canvas data to simulate incoming data stream'
AS
    CALL GENERATE_DUMMY_STUDENTS(10);  -- 10 new students per run

-- Additional task for course data (weekly)
CREATE OR REPLACE TASK TASK_GENERATE_DUMMY_COURSES
    WAREHOUSE = FGCU_TASK_WH
    SCHEDULE = 'USING CRON 0 6 * * 1 America/New_York'  -- Every Monday at 6 AM
    COMMENT = 'Generates new course data weekly'
AS
    CALL GENERATE_DUMMY_COURSES(5);  -- 5 new courses per week

-- ============================================================================
-- TASK 2: Process Raw Data Through Streams
-- ============================================================================

-- Task to process new student records
CREATE OR REPLACE TASK TASK_PROCESS_RAW_STUDENTS
    WAREHOUSE = FGCU_TRANSFORM_WH
    AFTER TASK_GENERATE_DUMMY_DATA
    WHEN SYSTEM$STREAM_HAS_DATA('STM_RAW_STUDENTS')
    COMMENT = 'Processes new student records from stream to curated layer'
AS
BEGIN
    -- Insert new/updated students into curated dimension
    MERGE INTO CURATED.DIM_STUDENTS tgt
    USING (
        SELECT 
            payload:student_id::VARCHAR AS student_id,
            payload:canvas_user_id::NUMBER AS canvas_user_id,
            payload:first_name::VARCHAR AS first_name,
            payload:last_name::VARCHAR AS last_name,
            payload:email::VARCHAR AS email,
            payload:major::VARCHAR AS major,
            payload:classification::VARCHAR AS classification,
            payload:enrollment_status::VARCHAR AS enrollment_status,
            payload:enrollment_date::DATE AS enrollment_date,
            payload:expected_graduation::DATE AS expected_graduation,
            payload:gpa::DECIMAL(3,2) AS gpa,
            payload:advisor_id::VARCHAR AS advisor_id
        FROM STM_RAW_STUDENTS
        WHERE METADATA$ACTION = 'INSERT'
    ) src
    ON tgt.student_id = src.student_id
    WHEN MATCHED THEN UPDATE SET
        canvas_user_id = src.canvas_user_id,
        first_name = src.first_name,
        last_name = src.last_name,
        email = src.email,
        major = src.major,
        classification = src.classification,
        enrollment_status = src.enrollment_status,
        gpa = src.gpa,
        advisor_id = src.advisor_id,
        updated_at = CURRENT_TIMESTAMP()
    WHEN NOT MATCHED THEN INSERT (
        student_id, canvas_user_id, first_name, last_name, email,
        major, classification, enrollment_status, enrollment_date,
        expected_graduation, gpa, advisor_id
    ) VALUES (
        src.student_id, src.canvas_user_id, src.first_name, src.last_name, src.email,
        src.major, src.classification, src.enrollment_status, src.enrollment_date,
        src.expected_graduation, src.gpa, src.advisor_id
    );
    
    -- Mark raw records as processed
    UPDATE RAW_STUDENTS
    SET processing_status = 'PROCESSED'
    WHERE processing_status = 'PENDING';
END;

-- Task to process new course records
CREATE OR REPLACE TASK TASK_PROCESS_RAW_COURSES
    WAREHOUSE = FGCU_TRANSFORM_WH
    AFTER TASK_GENERATE_DUMMY_COURSES
    WHEN SYSTEM$STREAM_HAS_DATA('STM_RAW_COURSES')
    COMMENT = 'Processes new course records from stream to curated layer'
AS
BEGIN
    MERGE INTO CURATED.DIM_COURSES tgt
    USING (
        SELECT 
            payload:course_id::VARCHAR AS course_id,
            payload:canvas_course_id::NUMBER AS canvas_course_id,
            payload:course_code::VARCHAR AS course_code,
            payload:course_name::VARCHAR AS course_name,
            payload:department::VARCHAR AS department,
            payload:credit_hours::NUMBER AS credit_hours,
            payload:course_level::VARCHAR AS course_level,
            payload:delivery_mode::VARCHAR AS delivery_mode,
            payload:term::VARCHAR AS term,
            payload:academic_year::VARCHAR AS academic_year,
            payload:instructor_id::VARCHAR AS instructor_id,
            payload:instructor_name::VARCHAR AS instructor_name,
            payload:start_date::DATE AS start_date,
            payload:end_date::DATE AS end_date,
            payload:max_enrollment::NUMBER AS max_enrollment
        FROM STM_RAW_COURSES
        WHERE METADATA$ACTION = 'INSERT'
    ) src
    ON tgt.course_id = src.course_id
    WHEN MATCHED THEN UPDATE SET
        canvas_course_id = src.canvas_course_id,
        course_code = src.course_code,
        course_name = src.course_name,
        department = src.department,
        credit_hours = src.credit_hours,
        course_level = src.course_level,
        delivery_mode = src.delivery_mode,
        term = src.term,
        academic_year = src.academic_year,
        instructor_id = src.instructor_id,
        instructor_name = src.instructor_name,
        start_date = src.start_date,
        end_date = src.end_date,
        max_enrollment = src.max_enrollment,
        updated_at = CURRENT_TIMESTAMP()
    WHEN NOT MATCHED THEN INSERT (
        course_id, canvas_course_id, course_code, course_name, department,
        credit_hours, course_level, delivery_mode, term, academic_year,
        instructor_id, instructor_name, start_date, end_date, max_enrollment
    ) VALUES (
        src.course_id, src.canvas_course_id, src.course_code, src.course_name, src.department,
        src.credit_hours, src.course_level, src.delivery_mode, src.term, src.academic_year,
        src.instructor_id, src.instructor_name, src.start_date, src.end_date, src.max_enrollment
    );
    
    UPDATE RAW_COURSES
    SET processing_status = 'PROCESSED'
    WHERE processing_status = 'PENDING';
END;

-- ============================================================================
-- TASK 3: Trigger Container Service Job
-- ============================================================================

-- This task triggers the SPCS container to run advanced data engineering
CREATE OR REPLACE TASK TASK_TRIGGER_CONTAINER_ETL
    WAREHOUSE = FGCU_TASK_WH
    SCHEDULE = 'USING CRON 0 */4 * * * America/New_York'  -- Every 4 hours
    COMMENT = 'Triggers the container-based ETL for complex transformations'
AS
BEGIN
    -- Log the trigger event
    INSERT INTO AUDIT.ETL_RUN_LOG (run_type, started_at, status)
    VALUES ('CONTAINER_ETL', CURRENT_TIMESTAMP(), 'TRIGGERED');
    
    -- The container service polls for this trigger or uses event-driven architecture
    -- In a real setup, this might use Snowflake's service functions or webhooks
    
    -- Alternative: Execute container service function directly
    -- SELECT COMPUTE.CANVAS_ETL_SERVICE!RUN_ETL();
END;

-- ============================================================================
-- TASK 4: Refresh Aggregation Tables
-- ============================================================================

CREATE OR REPLACE TASK TASK_REFRESH_AGGREGATIONS
    WAREHOUSE = FGCU_TRANSFORM_WH
    SCHEDULE = 'USING CRON 0 5 * * * America/New_York'  -- Daily at 5 AM
    COMMENT = 'Refreshes aggregation tables for analytics'
AS
BEGIN
    -- Refresh student course performance
    TRUNCATE TABLE CURATED.AGG_STUDENT_COURSE_PERFORMANCE;
    
    INSERT INTO CURATED.AGG_STUDENT_COURSE_PERFORMANCE
    SELECT 
        s.student_id,
        c.course_id,
        c.term,
        COUNT(DISTINCT a.assignment_id) AS total_assignments,
        COUNT(DISTINCT sub.submission_id) AS completed_assignments,
        AVG(sub.percentage) AS avg_score,
        SUM(sub.score) AS total_points_earned,
        SUM(sub.points_possible) AS total_points_possible,
        SUM(CASE WHEN sub.late_flag THEN 1 ELSE 0 END) AS late_submissions,
        SUM(CASE WHEN sub.missing_flag THEN 1 ELSE 0 END) AS missing_submissions,
        COALESCE(SUM(act.duration_seconds) / 60, 0) AS total_activity_minutes,
        MAX(act.activity_timestamp)::DATE AS last_activity_date,
        e.final_grade AS current_grade,
        CURRENT_TIMESTAMP() AS calculated_at
    FROM CURATED.DIM_STUDENTS s
    JOIN CURATED.FACT_ENROLLMENTS e ON s.student_id = e.student_id
    JOIN CURATED.DIM_COURSES c ON e.course_id = c.course_id
    LEFT JOIN CURATED.DIM_ASSIGNMENTS a ON a.course_id = c.course_id
    LEFT JOIN CURATED.FACT_SUBMISSIONS sub ON sub.student_id = s.student_id AND sub.assignment_id = a.assignment_id
    LEFT JOIN CURATED.FACT_ACTIVITY_LOGS act ON act.student_id = s.student_id AND act.course_id = c.course_id
    GROUP BY s.student_id, c.course_id, c.term, e.final_grade;
    
    -- Refresh course analytics
    TRUNCATE TABLE CURATED.AGG_COURSE_ANALYTICS;
    
    INSERT INTO CURATED.AGG_COURSE_ANALYTICS
    SELECT 
        c.course_id,
        c.term,
        COUNT(DISTINCT e.student_id) AS total_enrolled,
        COUNT(DISTINCT CASE WHEN e.enrollment_state = 'active' THEN e.student_id END) AS active_students,
        AVG(e.final_score) AS avg_class_score,
        MEDIAN(e.final_score) AS median_class_score,
        OBJECT_CONSTRUCT(
            'A', COUNT(CASE WHEN e.final_grade IN ('A', 'A-') THEN 1 END),
            'B', COUNT(CASE WHEN e.final_grade IN ('B+', 'B', 'B-') THEN 1 END),
            'C', COUNT(CASE WHEN e.final_grade IN ('C+', 'C', 'C-') THEN 1 END),
            'D', COUNT(CASE WHEN e.final_grade IN ('D+', 'D', 'D-') THEN 1 END),
            'F', COUNT(CASE WHEN e.final_grade = 'F' THEN 1 END)
        ) AS grade_distribution,
        ROUND(COUNT(CASE WHEN e.enrollment_state = 'completed' THEN 1 END) * 100.0 / NULLIF(COUNT(*), 0), 2) AS completion_rate,
        AVG(act_agg.total_minutes) AS avg_engagement_minutes,
        COUNT(CASE WHEN e.final_score < 60 THEN 1 END) AS at_risk_students,
        CURRENT_TIMESTAMP() AS calculated_at
    FROM CURATED.DIM_COURSES c
    JOIN CURATED.FACT_ENROLLMENTS e ON c.course_id = e.course_id
    LEFT JOIN (
        SELECT student_id, course_id, SUM(duration_seconds) / 60 AS total_minutes
        FROM CURATED.FACT_ACTIVITY_LOGS
        GROUP BY student_id, course_id
    ) act_agg ON act_agg.student_id = e.student_id AND act_agg.course_id = c.course_id
    GROUP BY c.course_id, c.term;
END;

-- ============================================================================
-- TASK 5: Data Quality Checks
-- ============================================================================

CREATE OR REPLACE TASK TASK_DATA_QUALITY_CHECKS
    WAREHOUSE = FGCU_TASK_WH
    SCHEDULE = 'USING CRON 0 6 * * * America/New_York'  -- Daily at 6 AM
    COMMENT = 'Runs data quality checks and logs issues'
AS
BEGIN
    -- Check for orphaned enrollments
    INSERT INTO AUDIT.DATA_QUALITY_LOG (check_name, check_timestamp, issues_found, details)
    SELECT 
        'ORPHANED_ENROLLMENTS',
        CURRENT_TIMESTAMP(),
        COUNT(*),
        OBJECT_CONSTRUCT('orphaned_records', ARRAY_AGG(e.enrollment_id))
    FROM CURATED.FACT_ENROLLMENTS e
    LEFT JOIN CURATED.DIM_STUDENTS s ON e.student_id = s.student_id
    WHERE s.student_id IS NULL;
    
    -- Check for duplicate student IDs
    INSERT INTO AUDIT.DATA_QUALITY_LOG (check_name, check_timestamp, issues_found, details)
    SELECT 
        'DUPLICATE_STUDENT_IDS',
        CURRENT_TIMESTAMP(),
        COUNT(*),
        OBJECT_CONSTRUCT('duplicate_ids', ARRAY_AGG(student_id))
    FROM (
        SELECT student_id
        FROM CURATED.DIM_STUDENTS
        GROUP BY student_id
        HAVING COUNT(*) > 1
    );
    
    -- Check for invalid GPA values
    INSERT INTO AUDIT.DATA_QUALITY_LOG (check_name, check_timestamp, issues_found, details)
    SELECT 
        'INVALID_GPA_VALUES',
        CURRENT_TIMESTAMP(),
        COUNT(*),
        OBJECT_CONSTRUCT('invalid_records', ARRAY_AGG(OBJECT_CONSTRUCT('student_id', student_id, 'gpa', gpa)))
    FROM CURATED.DIM_STUDENTS
    WHERE gpa < 0 OR gpa > 4.0;
END;

-- ============================================================================
-- CREATE AUDIT LOG TABLE
-- ============================================================================

USE SCHEMA AUDIT;

CREATE TABLE IF NOT EXISTS ETL_RUN_LOG (
    run_id          NUMBER AUTOINCREMENT PRIMARY KEY,
    run_type        VARCHAR(50),
    started_at      TIMESTAMP_NTZ,
    completed_at    TIMESTAMP_NTZ,
    status          VARCHAR(20),
    records_processed NUMBER,
    error_message   VARCHAR(5000),
    metadata        VARIANT
);

CREATE TABLE IF NOT EXISTS DATA_QUALITY_LOG (
    log_id          NUMBER AUTOINCREMENT PRIMARY KEY,
    check_name      VARCHAR(100),
    check_timestamp TIMESTAMP_NTZ,
    issues_found    NUMBER,
    details         VARIANT
);

-- ============================================================================
-- ENABLE TASKS (Run these when ready to start the pipeline)
-- ============================================================================

-- Start the task tree
-- ALTER TASK TASK_DATA_QUALITY_CHECKS RESUME;
-- ALTER TASK TASK_REFRESH_AGGREGATIONS RESUME;
-- ALTER TASK TASK_TRIGGER_CONTAINER_ETL RESUME;
-- ALTER TASK TASK_PROCESS_RAW_COURSES RESUME;
-- ALTER TASK TASK_PROCESS_RAW_STUDENTS RESUME;
-- ALTER TASK TASK_GENERATE_DUMMY_COURSES RESUME;
-- ALTER TASK TASK_GENERATE_DUMMY_DATA RESUME;

-- ============================================================================
-- TASK MANAGEMENT COMMANDS
-- ============================================================================

-- View all tasks
-- SHOW TASKS IN SCHEMA FGCU_CANVAS_DEMO.RAW;

-- Check task history
-- SELECT * FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY()) ORDER BY SCHEDULED_TIME DESC LIMIT 20;

-- Manually execute a task
-- EXECUTE TASK TASK_GENERATE_DUMMY_DATA;

-- Suspend all tasks
-- ALTER TASK TASK_GENERATE_DUMMY_DATA SUSPEND;

SELECT 'Scheduled tasks created successfully!' AS STATUS;



