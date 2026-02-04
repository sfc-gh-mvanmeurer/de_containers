/*
================================================================================
Demo Canvas Demo - Complete Reset and Regenerate Script
================================================================================
Run this script to:
1. Clear all existing data
2. Regenerate fresh synthetic data
3. Process to curated layer
4. Verify data integrity

Run AFTER updating 03_dummy_data_generator.sql and 04_scheduled_task.sql
================================================================================
*/

USE ROLE ACCOUNTADMIN;
USE DATABASE DEMO_CANVAS_DB;
USE WAREHOUSE DEMO_TRANSFORM_WH;

-- ============================================================================
-- STEP 1: CLEAR ALL DATA
-- ============================================================================

-- Clear RAW layer
TRUNCATE TABLE IF EXISTS RAW.RAW_STUDENTS;
TRUNCATE TABLE IF EXISTS RAW.RAW_COURSES;
TRUNCATE TABLE IF EXISTS RAW.RAW_ENROLLMENTS;
TRUNCATE TABLE IF EXISTS RAW.RAW_ASSIGNMENTS;
TRUNCATE TABLE IF EXISTS RAW.RAW_SUBMISSIONS;
TRUNCATE TABLE IF EXISTS RAW.RAW_ACTIVITY_LOGS;

-- Clear CURATED layer
TRUNCATE TABLE IF EXISTS CURATED.DIM_STUDENTS;
TRUNCATE TABLE IF EXISTS CURATED.DIM_COURSES;
TRUNCATE TABLE IF EXISTS CURATED.DIM_ASSIGNMENTS;
TRUNCATE TABLE IF EXISTS CURATED.FACT_ENROLLMENTS;
TRUNCATE TABLE IF EXISTS CURATED.FACT_SUBMISSIONS;
TRUNCATE TABLE IF EXISTS CURATED.FACT_ACTIVITY_LOGS;
TRUNCATE TABLE IF EXISTS CURATED.AGG_STUDENT_COURSE_PERFORMANCE;
TRUNCATE TABLE IF EXISTS CURATED.AGG_COURSE_ANALYTICS;

SELECT 'Step 1 Complete: All tables cleared' AS status;

-- ============================================================================
-- STEP 2: GENERATE RAW DATA
-- ============================================================================

-- Generate students first
CALL RAW.GENERATE_DUMMY_STUDENTS(200);
SELECT 'Generated students' AS status, COUNT(*) AS count FROM RAW.RAW_STUDENTS;

-- Generate courses
CALL RAW.GENERATE_DUMMY_COURSES(30);
SELECT 'Generated courses' AS status, COUNT(*) AS count FROM RAW.RAW_COURSES;

-- Generate enrollments (links students to courses)
CALL RAW.GENERATE_DUMMY_ENROLLMENTS(5);
SELECT 'Generated enrollments' AS status, COUNT(*) AS count FROM RAW.RAW_ENROLLMENTS;

-- Generate assignments
CALL RAW.GENERATE_DUMMY_ASSIGNMENTS(15);
SELECT 'Generated assignments' AS status, COUNT(*) AS count FROM RAW.RAW_ASSIGNMENTS;

-- Generate submissions
CALL RAW.GENERATE_DUMMY_SUBMISSIONS();
SELECT 'Generated submissions' AS status, COUNT(*) AS count FROM RAW.RAW_SUBMISSIONS;

-- Generate activity logs
CALL RAW.GENERATE_DUMMY_ACTIVITY_LOGS(10);
SELECT 'Generated activity logs' AS status, COUNT(*) AS count FROM RAW.RAW_ACTIVITY_LOGS;

SELECT 'Step 2 Complete: Raw data generated' AS status;

-- ============================================================================
-- STEP 3: VERIFY RAW DATA HAS VALID JSON
-- ============================================================================

-- Check students payload
SELECT 'RAW_STUDENTS sample' AS table_name, 
       PARSE_JSON(payload):student_id::VARCHAR AS student_id,
       PARSE_JSON(payload):first_name::VARCHAR AS first_name,
       PARSE_JSON(payload):major::VARCHAR AS major
FROM RAW.RAW_STUDENTS LIMIT 3;

-- Check courses payload
SELECT 'RAW_COURSES sample' AS table_name,
       PARSE_JSON(payload):course_id::VARCHAR AS course_id,
       PARSE_JSON(payload):course_name::VARCHAR AS course_name,
       PARSE_JSON(payload):department::VARCHAR AS department
FROM RAW.RAW_COURSES LIMIT 3;

-- Check enrollments payload
SELECT 'RAW_ENROLLMENTS sample' AS table_name,
       PARSE_JSON(payload):enrollment_id::VARCHAR AS enrollment_id,
       PARSE_JSON(payload):student_id::VARCHAR AS student_id,
       PARSE_JSON(payload):course_id::VARCHAR AS course_id
FROM RAW.RAW_ENROLLMENTS LIMIT 3;

SELECT 'Step 3 Complete: Raw data validated' AS status;

-- ============================================================================
-- STEP 4: PROCESS TO CURATED LAYER (ORDER MATTERS!)
-- ============================================================================

-- Process students first (no dependencies)
CALL RAW.PROC_PROCESS_RAW_STUDENTS();
SELECT 'Processed students' AS status, COUNT(*) AS count FROM CURATED.DIM_STUDENTS;

-- Process courses (no dependencies)
CALL RAW.PROC_PROCESS_RAW_COURSES();
SELECT 'Processed courses' AS status, COUNT(*) AS count FROM CURATED.DIM_COURSES;

-- Process assignments (depends on courses)
CALL RAW.PROC_PROCESS_RAW_ASSIGNMENTS();
SELECT 'Processed assignments' AS status, COUNT(*) AS count FROM CURATED.DIM_ASSIGNMENTS;

-- Process enrollments (depends on students + courses)
CALL RAW.PROC_PROCESS_RAW_ENROLLMENTS();
SELECT 'Processed enrollments' AS status, COUNT(*) AS count FROM CURATED.FACT_ENROLLMENTS;

-- Process submissions (depends on students + assignments)
CALL RAW.PROC_PROCESS_RAW_SUBMISSIONS();
SELECT 'Processed submissions' AS status, COUNT(*) AS count FROM CURATED.FACT_SUBMISSIONS;

-- Process activity logs (depends on students + courses)
CALL RAW.PROC_PROCESS_RAW_ACTIVITY_LOGS();
SELECT 'Processed activity logs' AS status, COUNT(*) AS count FROM CURATED.FACT_ACTIVITY_LOGS;

SELECT 'Step 4 Complete: Data processed to curated layer' AS status;

-- ============================================================================
-- STEP 5: VERIFY DATA LINKAGE
-- ============================================================================

-- Check enrollments have valid course linkage
SELECT 
    'Enrollment Linkage Check' AS check_name,
    COUNT(*) AS total_enrollments,
    COUNT(course_id) AS with_course_id,
    COUNT(course_key) AS with_course_key,
    COUNT(student_id) AS with_student_id,
    COUNT(student_key) AS with_student_key
FROM CURATED.FACT_ENROLLMENTS;

-- Sample enrollments with course details
SELECT 
    e.enrollment_id,
    e.student_id,
    e.course_id,
    c.course_code,
    c.course_name,
    c.department
FROM CURATED.FACT_ENROLLMENTS e
LEFT JOIN CURATED.DIM_COURSES c ON e.course_id = c.course_id
LIMIT 10;

-- ============================================================================
-- STEP 6: REFRESH AGGREGATIONS
-- ============================================================================

CALL RAW.PROC_REFRESH_AGGREGATIONS();

SELECT 'Step 6 Complete: Aggregations refreshed' AS status;

-- ============================================================================
-- STEP 7: FINAL DATA SUMMARY
-- ============================================================================

SELECT 'FINAL DATA COUNTS' AS summary;
SELECT 
    (SELECT COUNT(*) FROM CURATED.DIM_STUDENTS) AS students,
    (SELECT COUNT(*) FROM CURATED.DIM_COURSES) AS courses,
    (SELECT COUNT(*) FROM CURATED.DIM_ASSIGNMENTS) AS assignments,
    (SELECT COUNT(*) FROM CURATED.FACT_ENROLLMENTS) AS enrollments,
    (SELECT COUNT(*) FROM CURATED.FACT_SUBMISSIONS) AS submissions,
    (SELECT COUNT(*) FROM CURATED.FACT_ACTIVITY_LOGS) AS activity_logs,
    (SELECT COUNT(*) FROM CURATED.AGG_STUDENT_COURSE_PERFORMANCE) AS perf_aggregations;

-- Test query: Top courses by enrollment
SELECT 
    c.course_code,
    c.course_name,
    c.department,
    COUNT(*) AS enrollment_count
FROM CURATED.FACT_ENROLLMENTS e
JOIN CURATED.DIM_COURSES c ON e.course_id = c.course_id
GROUP BY c.course_code, c.course_name, c.department
ORDER BY enrollment_count DESC
LIMIT 10;

SELECT '✅ Reset and regeneration complete!' AS status;

