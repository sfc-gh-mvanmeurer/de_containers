/*
================================================================================
Demo Canvas Analytics - Cortex Search Setup
================================================================================
Sets up Cortex Search services for unstructured data and document search
to complement the semantic views for structured data analysis.

This enables the Snowflake Intelligence agent to search through:
- Activity logs
- Course catalog
- Student directory

Reference: https://docs.snowflake.com/en/user-guide/snowflake-cortex/cortex-search
================================================================================
*/

-- ============================================================================
-- SETUP
-- ============================================================================
USE ROLE ACCOUNTADMIN;
USE DATABASE DEMO_CANVAS_DB;
USE WAREHOUSE DEMO_TRANSFORM_WH;
USE SCHEMA ANALYTICS;

-- ============================================================================
-- CORTEX SEARCH SERVICE 1: ACTIVITY LOG SEARCH
-- Enables natural language search over student activity logs
-- ============================================================================

-- First, create a view that prepares activity log data for search
CREATE OR REPLACE VIEW ACTIVITY_LOG_SEARCH_SOURCE AS
SELECT 
    activity_id,
    student_id,
    course_id,
    activity_type,
    activity_timestamp,
    duration_seconds,
    page_url,
    device_type,
    browser,
    -- Combine fields into searchable text
    activity_type || ' activity on ' || COALESCE(page_url, 'unknown page') || 
    ' using ' || COALESCE(device_type, 'unknown device') || 
    ' (' || COALESCE(browser, 'unknown browser') || ')' ||
    ' for ' || COALESCE(TO_VARCHAR(duration_seconds), '0') || ' seconds' AS search_text,
    -- Metadata for filtering
    DATE(activity_timestamp) AS activity_date
FROM DEMO_CANVAS_DB.CURATED.FACT_ACTIVITY_LOGS;

-- Create Cortex Search Service for activity logs
CREATE OR REPLACE CORTEX SEARCH SERVICE CANVAS_ACTIVITY_SEARCH
    ON search_text
    ATTRIBUTES activity_type, activity_date
    WAREHOUSE = DEMO_TRANSFORM_WH
    TARGET_LAG = '1 hour'
    COMMENT = 'Search service for student activity logs in Canvas LMS'
AS (
    SELECT 
        activity_id,
        student_id,
        course_id,
        activity_type,
        activity_timestamp,
        duration_seconds,
        page_url,
        device_type,
        browser,
        search_text,
        activity_date
    FROM ACTIVITY_LOG_SEARCH_SOURCE
);

-- Grant access
GRANT USAGE ON CORTEX SEARCH SERVICE CANVAS_ACTIVITY_SEARCH TO ROLE PUBLIC;


-- ============================================================================
-- CORTEX SEARCH SERVICE 2: COURSE CATALOG SEARCH
-- Enables natural language search over course information
-- ============================================================================

-- Create a view for course catalog search
CREATE OR REPLACE VIEW COURSE_CATALOG_SEARCH_SOURCE AS
SELECT 
    course_id,
    canvas_course_id,
    course_code,
    course_name,
    department,
    credit_hours,
    course_level,
    delivery_mode,
    term,
    academic_year,
    instructor_name,
    start_date,
    end_date,
    max_enrollment,
    -- Combine fields into searchable text
    course_code || ' - ' || course_name || '. ' ||
    'Department: ' || COALESCE(department, 'Unknown') || '. ' ||
    'Instructor: ' || COALESCE(instructor_name, 'TBD') || '. ' ||
    'Credits: ' || COALESCE(TO_VARCHAR(credit_hours), 'N/A') || '. ' ||
    'Level: ' || COALESCE(course_level, 'Unknown') || '. ' ||
    'Delivery: ' || COALESCE(delivery_mode, 'Unknown') || '. ' ||
    'Term: ' || COALESCE(term, 'Unknown') AS search_text
FROM DEMO_CANVAS_DB.CURATED.DIM_COURSES;

-- Create Cortex Search Service for course catalog
CREATE OR REPLACE CORTEX SEARCH SERVICE CANVAS_COURSE_SEARCH
    ON search_text
    ATTRIBUTES department, term, course_level, delivery_mode
    WAREHOUSE = DEMO_TRANSFORM_WH
    TARGET_LAG = '1 day'
    COMMENT = 'Search service for Canvas LMS course catalog'
AS (
    SELECT 
        course_id,
        canvas_course_id,
        course_code,
        course_name,
        department,
        credit_hours,
        course_level,
        delivery_mode,
        term,
        academic_year,
        instructor_name,
        start_date,
        end_date,
        max_enrollment,
        search_text
    FROM COURSE_CATALOG_SEARCH_SOURCE
);

GRANT USAGE ON CORTEX SEARCH SERVICE CANVAS_COURSE_SEARCH TO ROLE PUBLIC;


-- ============================================================================
-- CORTEX SEARCH SERVICE 3: STUDENT DIRECTORY SEARCH
-- Enables natural language search over student information
-- ============================================================================

-- Create a view for student directory search
CREATE OR REPLACE VIEW STUDENT_DIRECTORY_SEARCH_SOURCE AS
SELECT 
    student_id,
    canvas_user_id,
    first_name,
    last_name,
    first_name || ' ' || last_name AS full_name,
    email,
    major,
    classification,
    enrollment_status,
    gpa,
    advisor_id,
    enrollment_date,
    expected_graduation,
    -- Combine fields into searchable text
    first_name || ' ' || last_name || '. ' ||
    'Major: ' || COALESCE(major, 'Undeclared') || '. ' ||
    'Classification: ' || COALESCE(classification, 'Unknown') || '. ' ||
    'Status: ' || COALESCE(enrollment_status, 'Unknown') || '. ' ||
    'GPA: ' || COALESCE(TO_VARCHAR(ROUND(gpa, 2)), 'N/A') || '. ' ||
    'Email: ' || COALESCE(email, 'N/A') AS search_text
FROM DEMO_CANVAS_DB.CURATED.DIM_STUDENTS;

-- Create Cortex Search Service for student directory
CREATE OR REPLACE CORTEX SEARCH SERVICE CANVAS_STUDENT_SEARCH
    ON search_text
    ATTRIBUTES major, classification, enrollment_status
    WAREHOUSE = DEMO_TRANSFORM_WH
    TARGET_LAG = '1 day'
    COMMENT = 'Search service for Canvas LMS student directory'
AS (
    SELECT 
        student_id,
        canvas_user_id,
        first_name,
        last_name,
        full_name,
        email,
        major,
        classification,
        enrollment_status,
        gpa,
        advisor_id,
        enrollment_date,
        expected_graduation,
        search_text
    FROM STUDENT_DIRECTORY_SEARCH_SOURCE
);

GRANT USAGE ON CORTEX SEARCH SERVICE CANVAS_STUDENT_SEARCH TO ROLE PUBLIC;


-- ============================================================================
-- VERIFICATION
-- ============================================================================

-- Show all Cortex Search services
SHOW CORTEX SEARCH SERVICES IN SCHEMA DEMO_CANVAS_DB.ANALYTICS;

-- Show the source views
SHOW VIEWS LIKE '%SEARCH_SOURCE%' IN SCHEMA DEMO_CANVAS_DB.ANALYTICS;

SELECT 'Cortex Search services setup complete!' AS status;
