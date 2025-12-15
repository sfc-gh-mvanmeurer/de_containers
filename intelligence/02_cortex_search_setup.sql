/*
================================================================================
FGCU Canvas Analytics - Cortex Search Setup
================================================================================
Sets up Cortex Search services for unstructured data and document search
to complement the semantic views for structured data analysis.

This enables the Snowflake Intelligence agent to search through:
- Activity logs and descriptions
- Course syllabi and materials (if available)
- Student notes and comments

Reference: https://docs.snowflake.com/en/user-guide/snowflake-cortex/cortex-search
================================================================================
*/

-- ============================================================================
-- SETUP
-- ============================================================================
USE ROLE ACCOUNTADMIN;
USE DATABASE FGCU_CANVAS_DEMO;
USE WAREHOUSE FGCU_TRANSFORM_WH;
USE SCHEMA ANALYTICS;

-- ============================================================================
-- CORTEX SEARCH SERVICE 1: ACTIVITY LOG SEARCH
-- Enables natural language search over student activity descriptions
-- ============================================================================

-- First, create a view that prepares activity log data for search
CREATE OR REPLACE VIEW ACTIVITY_LOG_SEARCH_SOURCE AS
SELECT 
    activity_id,
    student_id,
    course_id,
    activity_type,
    activity_description,
    activity_timestamp,
    -- Combine fields into searchable text
    activity_type || ': ' || COALESCE(activity_description, '') AS search_text,
    -- Metadata for filtering
    DATE(activity_timestamp) AS activity_date
FROM FGCU_CANVAS_DEMO.CURATED.FACT_ACTIVITY_LOGS
WHERE activity_description IS NOT NULL;

-- Create Cortex Search Service for activity logs
CREATE OR REPLACE CORTEX SEARCH SERVICE CANVAS_ACTIVITY_SEARCH
    ON search_text
    ATTRIBUTES activity_type, activity_date
    WAREHOUSE = FGCU_TRANSFORM_WH
    TARGET_LAG = '1 hour'
    COMMENT = 'Search service for student activity logs in Canvas LMS'
AS (
    SELECT 
        activity_id,
        student_id,
        course_id,
        activity_type,
        activity_description,
        activity_timestamp,
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
    credits,
    term,
    instructor_name,
    status,
    start_date,
    end_date,
    -- Combine fields into searchable text
    course_code || ' - ' || course_name || '. ' ||
    'Department: ' || department || '. ' ||
    'Instructor: ' || COALESCE(instructor_name, 'TBD') || '. ' ||
    'Credits: ' || credits || '. ' ||
    'Term: ' || term AS search_text
FROM FGCU_CANVAS_DEMO.CURATED.DIM_COURSES;

-- Create Cortex Search Service for course catalog
CREATE OR REPLACE CORTEX SEARCH SERVICE CANVAS_COURSE_SEARCH
    ON search_text
    ATTRIBUTES department, term, status
    WAREHOUSE = FGCU_TRANSFORM_WH
    TARGET_LAG = '1 day'
    COMMENT = 'Search service for Canvas LMS course catalog'
AS (
    SELECT 
        course_id,
        canvas_course_id,
        course_code,
        course_name,
        department,
        credits,
        term,
        instructor_name,
        status,
        start_date,
        end_date,
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
    -- Combine fields into searchable text
    first_name || ' ' || last_name || '. ' ||
    'Major: ' || major || '. ' ||
    'Classification: ' || classification || '. ' ||
    'Status: ' || enrollment_status || '. ' ||
    'GPA: ' || ROUND(gpa, 2) AS search_text
FROM FGCU_CANVAS_DEMO.CURATED.DIM_STUDENTS;

-- Create Cortex Search Service for student directory
CREATE OR REPLACE CORTEX SEARCH SERVICE CANVAS_STUDENT_SEARCH
    ON search_text
    ATTRIBUTES major, classification, enrollment_status
    WAREHOUSE = FGCU_TRANSFORM_WH
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
        search_text
    FROM STUDENT_DIRECTORY_SEARCH_SOURCE
);

GRANT USAGE ON CORTEX SEARCH SERVICE CANVAS_STUDENT_SEARCH TO ROLE PUBLIC;


-- ============================================================================
-- HELPER FUNCTIONS FOR SEARCH
-- ============================================================================

-- Function to search activities by natural language query
CREATE OR REPLACE FUNCTION SEARCH_ACTIVITIES(query STRING, max_results INT DEFAULT 10)
RETURNS TABLE (
    activity_id STRING,
    student_id STRING,
    course_id STRING,
    activity_type STRING,
    activity_description STRING,
    activity_timestamp TIMESTAMP,
    relevance_score FLOAT
)
AS
$$
    SELECT 
        activity_id,
        student_id,
        course_id,
        activity_type,
        activity_description,
        activity_timestamp,
        score AS relevance_score
    FROM TABLE(
        CORTEX_SEARCH(
            'CANVAS_ACTIVITY_SEARCH',
            query,
            max_results
        )
    )
$$;

-- Function to search courses by natural language query
CREATE OR REPLACE FUNCTION SEARCH_COURSES(query STRING, max_results INT DEFAULT 10)
RETURNS TABLE (
    course_id STRING,
    course_code STRING,
    course_name STRING,
    department STRING,
    instructor_name STRING,
    term STRING,
    relevance_score FLOAT
)
AS
$$
    SELECT 
        course_id,
        course_code,
        course_name,
        department,
        instructor_name,
        term,
        score AS relevance_score
    FROM TABLE(
        CORTEX_SEARCH(
            'CANVAS_COURSE_SEARCH',
            query,
            max_results
        )
    )
$$;

-- Function to search students by natural language query
CREATE OR REPLACE FUNCTION SEARCH_STUDENTS(query STRING, max_results INT DEFAULT 10)
RETURNS TABLE (
    student_id STRING,
    full_name STRING,
    major STRING,
    classification STRING,
    enrollment_status STRING,
    gpa FLOAT,
    relevance_score FLOAT
)
AS
$$
    SELECT 
        student_id,
        full_name,
        major,
        classification,
        enrollment_status,
        gpa,
        score AS relevance_score
    FROM TABLE(
        CORTEX_SEARCH(
            'CANVAS_STUDENT_SEARCH',
            query,
            max_results
        )
    )
$$;


-- ============================================================================
-- VERIFICATION
-- ============================================================================

-- Show all Cortex Search services
SHOW CORTEX SEARCH SERVICES IN SCHEMA FGCU_CANVAS_DEMO.ANALYTICS;

-- Test the search services (uncomment to test)
-- SELECT * FROM TABLE(SEARCH_ACTIVITIES('quiz submission', 5));
-- SELECT * FROM TABLE(SEARCH_COURSES('computer science programming', 5));
-- SELECT * FROM TABLE(SEARCH_STUDENTS('computer science senior', 5));

PRINT '✅ Cortex Search services created successfully!';
PRINT 'Next step: Run 03_intelligence_agent.sql';

