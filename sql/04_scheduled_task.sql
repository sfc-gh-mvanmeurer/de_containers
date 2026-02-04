/*
================================================================================
Demo Canvas Data Engineering - Scheduled Tasks
================================================================================
This script creates tasks that orchestrate the data pipeline:
1. Generate dummy data periodically
2. Trigger the container-based ETL process
3. Refresh aggregations
================================================================================
*/

USE DATABASE DEMO_CANVAS_DB;
USE SCHEMA RAW;
USE WAREHOUSE DEMO_TASK_WH;

-- ============================================================================
-- CREATE AUDIT LOG TABLES FIRST (needed by procedures)
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

USE SCHEMA RAW;

-- ============================================================================
-- STORED PROCEDURES FOR MULTI-STATEMENT TASKS
-- ============================================================================

-- Procedure to process raw students (uses PARSE_JSON for string payload)
CREATE OR REPLACE PROCEDURE PROC_PROCESS_RAW_STUDENTS()
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
BEGIN
    -- Insert new/updated students into curated dimension
    MERGE INTO CURATED.DIM_STUDENTS tgt
    USING (
        SELECT 
            PARSE_JSON(payload):student_id::VARCHAR AS student_id,
            PARSE_JSON(payload):canvas_user_id::NUMBER AS canvas_user_id,
            PARSE_JSON(payload):first_name::VARCHAR AS first_name,
            PARSE_JSON(payload):last_name::VARCHAR AS last_name,
            PARSE_JSON(payload):email::VARCHAR AS email,
            PARSE_JSON(payload):major::VARCHAR AS major,
            PARSE_JSON(payload):classification::VARCHAR AS classification,
            PARSE_JSON(payload):enrollment_status::VARCHAR AS enrollment_status,
            PARSE_JSON(payload):enrollment_date::DATE AS enrollment_date,
            PARSE_JSON(payload):expected_graduation::DATE AS expected_graduation,
            PARSE_JSON(payload):gpa::DECIMAL(3,2) AS gpa,
            PARSE_JSON(payload):advisor_id::VARCHAR AS advisor_id
        FROM RAW_STUDENTS
        WHERE processing_status = 'PENDING'
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
    
    RETURN 'Students processed successfully';
END;
$$;

-- Procedure to process raw courses (uses PARSE_JSON for string payload)
CREATE OR REPLACE PROCEDURE PROC_PROCESS_RAW_COURSES()
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
BEGIN
    MERGE INTO CURATED.DIM_COURSES tgt
    USING (
        SELECT 
            PARSE_JSON(payload):course_id::VARCHAR AS course_id,
            PARSE_JSON(payload):canvas_course_id::NUMBER AS canvas_course_id,
            PARSE_JSON(payload):course_code::VARCHAR AS course_code,
            PARSE_JSON(payload):course_name::VARCHAR AS course_name,
            PARSE_JSON(payload):department::VARCHAR AS department,
            PARSE_JSON(payload):credit_hours::NUMBER AS credit_hours,
            PARSE_JSON(payload):course_level::VARCHAR AS course_level,
            PARSE_JSON(payload):delivery_mode::VARCHAR AS delivery_mode,
            PARSE_JSON(payload):term::VARCHAR AS term,
            PARSE_JSON(payload):academic_year::VARCHAR AS academic_year,
            PARSE_JSON(payload):instructor_id::VARCHAR AS instructor_id,
            PARSE_JSON(payload):instructor_name::VARCHAR AS instructor_name,
            PARSE_JSON(payload):start_date::DATE AS start_date,
            PARSE_JSON(payload):end_date::DATE AS end_date,
            PARSE_JSON(payload):max_enrollment::NUMBER AS max_enrollment
        FROM RAW_COURSES
        WHERE processing_status = 'PENDING'
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
    
    RETURN 'Courses processed successfully';
END;
$$;

-- Procedure to process raw enrollments (uses PARSE_JSON for string payload)
CREATE OR REPLACE PROCEDURE PROC_PROCESS_RAW_ENROLLMENTS()
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
BEGIN
    MERGE INTO CURATED.FACT_ENROLLMENTS tgt
    USING (
        SELECT 
            PARSE_JSON(r.payload):enrollment_id::VARCHAR AS enrollment_id,
            s.student_key,
            c.course_key,
            PARSE_JSON(r.payload):student_id::VARCHAR AS student_id,
            PARSE_JSON(r.payload):course_id::VARCHAR AS course_id,
            PARSE_JSON(r.payload):enrollment_state::VARCHAR AS enrollment_state,
            PARSE_JSON(r.payload):enrollment_type::VARCHAR AS enrollment_type,
            PARSE_JSON(r.payload):enrolled_at::TIMESTAMP_NTZ AS enrolled_at,
            PARSE_JSON(r.payload):final_grade::VARCHAR AS final_grade,
            PARSE_JSON(r.payload):final_score::DECIMAL(5,2) AS final_score
        FROM RAW_ENROLLMENTS r
        LEFT JOIN CURATED.DIM_STUDENTS s ON PARSE_JSON(r.payload):student_id::VARCHAR = s.student_id
        LEFT JOIN CURATED.DIM_COURSES c ON PARSE_JSON(r.payload):course_id::VARCHAR = c.course_id
        WHERE r.processing_status = 'PENDING'
    ) src
    ON tgt.enrollment_id = src.enrollment_id
    WHEN MATCHED THEN UPDATE SET
        enrollment_state = src.enrollment_state,
        final_grade = src.final_grade,
        final_score = src.final_score,
        updated_at = CURRENT_TIMESTAMP()
    WHEN NOT MATCHED THEN INSERT (
        enrollment_id, student_key, course_key, student_id, course_id,
        enrollment_state, enrollment_type, enrolled_at, final_grade, final_score
    ) VALUES (
        src.enrollment_id, src.student_key, src.course_key, src.student_id, src.course_id,
        src.enrollment_state, src.enrollment_type, src.enrolled_at, src.final_grade, src.final_score
    );
    
    UPDATE RAW_ENROLLMENTS
    SET processing_status = 'PROCESSED'
    WHERE processing_status = 'PENDING';
    
    RETURN 'Enrollments processed successfully';
END;
$$;

-- Procedure to process raw assignments (uses PARSE_JSON for string payload)
CREATE OR REPLACE PROCEDURE PROC_PROCESS_RAW_ASSIGNMENTS()
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
BEGIN
    MERGE INTO CURATED.DIM_ASSIGNMENTS tgt
    USING (
        SELECT 
            PARSE_JSON(payload):assignment_id::VARCHAR AS assignment_id,
            PARSE_JSON(payload):canvas_assignment_id::NUMBER AS canvas_assignment_id,
            PARSE_JSON(payload):course_id::VARCHAR AS course_id,
            PARSE_JSON(payload):assignment_name::VARCHAR AS assignment_name,
            PARSE_JSON(payload):assignment_type::VARCHAR AS assignment_type,
            PARSE_JSON(payload):points_possible::DECIMAL(10,2) AS points_possible,
            PARSE_JSON(payload):due_date::TIMESTAMP_NTZ AS due_date,
            PARSE_JSON(payload):unlock_date::TIMESTAMP_NTZ AS unlock_date,
            PARSE_JSON(payload):lock_date::TIMESTAMP_NTZ AS lock_date,
            PARSE_JSON(payload):submission_types::VARCHAR AS submission_types,
            PARSE_JSON(payload):is_group_assignment::BOOLEAN AS is_group_assignment,
            PARSE_JSON(payload):weight::DECIMAL(5,2) AS weight
        FROM RAW_ASSIGNMENTS
        WHERE processing_status = 'PENDING'
    ) src
    ON tgt.assignment_id = src.assignment_id
    WHEN MATCHED THEN UPDATE SET
        assignment_name = src.assignment_name,
        points_possible = src.points_possible,
        due_date = src.due_date,
        weight = src.weight,
        updated_at = CURRENT_TIMESTAMP()
    WHEN NOT MATCHED THEN INSERT (
        assignment_id, canvas_assignment_id, course_id, assignment_name,
        assignment_type, points_possible, due_date, unlock_date, lock_date,
        submission_types, is_group_assignment, weight
    ) VALUES (
        src.assignment_id, src.canvas_assignment_id, src.course_id, src.assignment_name,
        src.assignment_type, src.points_possible, src.due_date, src.unlock_date, src.lock_date,
        src.submission_types, src.is_group_assignment, src.weight
    );
    
    UPDATE RAW_ASSIGNMENTS
    SET processing_status = 'PROCESSED'
    WHERE processing_status = 'PENDING';
    
    RETURN 'Assignments processed successfully';
END;
$$;

-- Procedure to process raw submissions (uses PARSE_JSON for string payload)
CREATE OR REPLACE PROCEDURE PROC_PROCESS_RAW_SUBMISSIONS()
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
BEGIN
    MERGE INTO CURATED.FACT_SUBMISSIONS tgt
    USING (
        SELECT 
            PARSE_JSON(r.payload):submission_id::VARCHAR AS submission_id,
            s.student_key,
            a.assignment_key,
            PARSE_JSON(r.payload):student_id::VARCHAR AS student_id,
            PARSE_JSON(r.payload):assignment_id::VARCHAR AS assignment_id,
            PARSE_JSON(r.payload):submitted_at::TIMESTAMP_NTZ AS submitted_at,
            PARSE_JSON(r.payload):graded_at::TIMESTAMP_NTZ AS graded_at,
            PARSE_JSON(r.payload):score::DECIMAL(10,2) AS score,
            PARSE_JSON(r.payload):grade::VARCHAR AS grade,
            PARSE_JSON(r.payload):points_possible::DECIMAL(10,2) AS points_possible,
            PARSE_JSON(r.payload):percentage::DECIMAL(5,2) AS percentage,
            PARSE_JSON(r.payload):submission_type::VARCHAR AS submission_type,
            PARSE_JSON(r.payload):attempt_number::NUMBER AS attempt_number,
            PARSE_JSON(r.payload):late_flag::BOOLEAN AS late_flag,
            PARSE_JSON(r.payload):missing_flag::BOOLEAN AS missing_flag,
            PARSE_JSON(r.payload):excused_flag::BOOLEAN AS excused_flag,
            PARSE_JSON(r.payload):grader_id::VARCHAR AS grader_id
        FROM RAW_SUBMISSIONS r
        LEFT JOIN CURATED.DIM_STUDENTS s ON PARSE_JSON(r.payload):student_id::VARCHAR = s.student_id
        LEFT JOIN CURATED.DIM_ASSIGNMENTS a ON PARSE_JSON(r.payload):assignment_id::VARCHAR = a.assignment_id
        WHERE r.processing_status = 'PENDING'
    ) src
    ON tgt.submission_id = src.submission_id
    WHEN MATCHED THEN UPDATE SET
        graded_at = src.graded_at,
        score = src.score,
        grade = src.grade,
        percentage = src.percentage,
        updated_at = CURRENT_TIMESTAMP()
    WHEN NOT MATCHED THEN INSERT (
        submission_id, student_key, assignment_key, student_id, assignment_id,
        submitted_at, graded_at, score, grade, points_possible, percentage,
        submission_type, attempt_number, late_flag, missing_flag, excused_flag, grader_id
    ) VALUES (
        src.submission_id, src.student_key, src.assignment_key, src.student_id, src.assignment_id,
        src.submitted_at, src.graded_at, src.score, src.grade, src.points_possible, src.percentage,
        src.submission_type, src.attempt_number, src.late_flag, src.missing_flag, src.excused_flag, src.grader_id
    );
    
    UPDATE RAW_SUBMISSIONS
    SET processing_status = 'PROCESSED'
    WHERE processing_status = 'PENDING';
    
    RETURN 'Submissions processed successfully';
END;
$$;

-- Procedure to process raw activity logs (uses PARSE_JSON for string payload)
CREATE OR REPLACE PROCEDURE PROC_PROCESS_RAW_ACTIVITY_LOGS()
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
BEGIN
    MERGE INTO CURATED.FACT_ACTIVITY_LOGS tgt
    USING (
        SELECT 
            PARSE_JSON(r.payload):activity_id::VARCHAR AS activity_id,
            s.student_key,
            c.course_key,
            PARSE_JSON(r.payload):student_id::VARCHAR AS student_id,
            PARSE_JSON(r.payload):course_id::VARCHAR AS course_id,
            PARSE_JSON(r.payload):activity_type::VARCHAR AS activity_type,
            PARSE_JSON(r.payload):activity_timestamp::TIMESTAMP_NTZ AS activity_timestamp,
            PARSE_JSON(r.payload):duration_seconds::NUMBER AS duration_seconds,
            PARSE_JSON(r.payload):page_url::VARCHAR AS page_url,
            PARSE_JSON(r.payload):device_type::VARCHAR AS device_type,
            PARSE_JSON(r.payload):browser::VARCHAR AS browser,
            PARSE_JSON(r.payload):ip_address::VARCHAR AS ip_address
        FROM RAW_ACTIVITY_LOGS r
        LEFT JOIN CURATED.DIM_STUDENTS s ON PARSE_JSON(r.payload):student_id::VARCHAR = s.student_id
        LEFT JOIN CURATED.DIM_COURSES c ON PARSE_JSON(r.payload):course_id::VARCHAR = c.course_id
        WHERE r.processing_status = 'PENDING'
    ) src
    ON tgt.activity_id = src.activity_id
    WHEN NOT MATCHED THEN INSERT (
        activity_id, student_key, course_key, student_id, course_id,
        activity_type, activity_timestamp, duration_seconds, page_url,
        device_type, browser, ip_address
    ) VALUES (
        src.activity_id, src.student_key, src.course_key, src.student_id, src.course_id,
        src.activity_type, src.activity_timestamp, src.duration_seconds, src.page_url,
        src.device_type, src.browser, src.ip_address
    );
    
    UPDATE RAW_ACTIVITY_LOGS
    SET processing_status = 'PROCESSED'
    WHERE processing_status = 'PENDING';
    
    RETURN 'Activity logs processed successfully';
END;
$$;


-- Procedure to trigger container ETL
CREATE OR REPLACE PROCEDURE PROC_TRIGGER_CONTAINER_ETL()
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
BEGIN
    -- Log the trigger event
    INSERT INTO AUDIT.ETL_RUN_LOG (run_type, started_at, status)
    VALUES ('CONTAINER_ETL', CURRENT_TIMESTAMP(), 'TRIGGERED');
    
    RETURN 'Container ETL triggered';
END;
$$;

-- Procedure to refresh aggregations
CREATE OR REPLACE PROCEDURE PROC_REFRESH_AGGREGATIONS()
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
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
    
    RETURN 'Aggregations refreshed successfully';
END;
$$;

-- Procedure for data quality checks
CREATE OR REPLACE PROCEDURE PROC_DATA_QUALITY_CHECKS()
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
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
    
    RETURN 'Data quality checks completed';
END;
$$;

-- Master procedure to process all raw data
CREATE OR REPLACE PROCEDURE PROC_PROCESS_ALL_RAW_DATA()
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
BEGIN
    CALL PROC_PROCESS_RAW_STUDENTS();
    CALL PROC_PROCESS_RAW_COURSES();
    CALL PROC_PROCESS_RAW_ENROLLMENTS();
    CALL PROC_PROCESS_RAW_ASSIGNMENTS();
    CALL PROC_PROCESS_RAW_SUBMISSIONS();
    CALL PROC_PROCESS_RAW_ACTIVITY_LOGS();
    RETURN 'All raw data processed successfully';
END;
$$;

-- ============================================================================
-- TASK 1: Generate New Dummy Data (Simulates incoming Canvas data)
-- ============================================================================

CREATE OR REPLACE TASK TASK_GENERATE_DUMMY_DATA
    WAREHOUSE = DEMO_TASK_WH
    SCHEDULE = 'USING CRON 0 */2 * * * America/New_York'
    COMMENT = 'Generates synthetic Canvas data to simulate incoming data stream'
AS
    CALL GENERATE_DUMMY_STUDENTS(10);

-- Additional task for course data (weekly)
CREATE OR REPLACE TASK TASK_GENERATE_DUMMY_COURSES
    WAREHOUSE = DEMO_TASK_WH
    SCHEDULE = 'USING CRON 0 6 * * 1 America/New_York'
    COMMENT = 'Generates new course data weekly'
AS
    CALL GENERATE_DUMMY_COURSES(5);

-- ============================================================================
-- TASK 2: Process Raw Data
-- ============================================================================

CREATE OR REPLACE TASK TASK_PROCESS_RAW_STUDENTS
    WAREHOUSE = DEMO_TRANSFORM_WH
    COMMENT = 'Processes new student records to curated layer'
    AFTER TASK_GENERATE_DUMMY_DATA
AS
    CALL PROC_PROCESS_RAW_STUDENTS();

CREATE OR REPLACE TASK TASK_PROCESS_RAW_COURSES
    WAREHOUSE = DEMO_TRANSFORM_WH
    COMMENT = 'Processes new course records to curated layer'
    AFTER TASK_GENERATE_DUMMY_COURSES
AS
    CALL PROC_PROCESS_RAW_COURSES();

-- ============================================================================
-- TASK 3: Trigger Container Service Job
-- ============================================================================

CREATE OR REPLACE TASK TASK_TRIGGER_CONTAINER_ETL
    WAREHOUSE = DEMO_TASK_WH
    SCHEDULE = 'USING CRON 0 */4 * * * America/New_York'
    COMMENT = 'Triggers the container-based ETL for complex transformations'
AS
    CALL PROC_TRIGGER_CONTAINER_ETL();

-- ============================================================================
-- TASK 4: Refresh Aggregation Tables
-- ============================================================================

CREATE OR REPLACE TASK TASK_REFRESH_AGGREGATIONS
    WAREHOUSE = DEMO_TRANSFORM_WH
    SCHEDULE = 'USING CRON 0 5 * * * America/New_York'
    COMMENT = 'Refreshes aggregation tables for analytics'
AS
    CALL PROC_REFRESH_AGGREGATIONS();

-- ============================================================================
-- TASK 5: Data Quality Checks
-- ============================================================================

CREATE OR REPLACE TASK TASK_DATA_QUALITY_CHECKS
    WAREHOUSE = DEMO_TASK_WH
    SCHEDULE = 'USING CRON 0 6 * * * America/New_York'
    COMMENT = 'Runs data quality checks and logs issues'
AS
    CALL PROC_DATA_QUALITY_CHECKS();

-- ============================================================================
-- ENABLE TASKS (Run these when ready to start the pipeline)
-- ============================================================================

-- Start the task tree (run in reverse dependency order)
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
-- SHOW TASKS IN SCHEMA DEMO_CANVAS_DB.RAW;

-- Check task history
-- SELECT * FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY()) ORDER BY SCHEDULED_TIME DESC LIMIT 20;

-- Manually execute a task
-- EXECUTE TASK TASK_GENERATE_DUMMY_DATA;

-- Suspend all tasks
-- ALTER TASK TASK_GENERATE_DUMMY_DATA SUSPEND;

SELECT 'Scheduled tasks created successfully!' AS STATUS;
