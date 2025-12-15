/*
================================================================================
FGCU Canvas Analytics - Snowflake Intelligence Agent Setup
================================================================================
Configures a Snowflake Intelligence agent that uses:
- Semantic Views for structured data analysis via Cortex Analyst
- Cortex Search services for unstructured data retrieval

The agent can answer natural language questions about:
- Student performance and demographics
- Course enrollment and grades
- Assignment submissions and completion rates
- Activity patterns and engagement

Reference: https://docs.snowflake.com/en/user-guide/snowflake-cortex/snowflake-intelligence
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
-- AGENT CONFIGURATION
-- ============================================================================

/*
IMPORTANT: Snowflake Intelligence agents are created through the Snowsight UI.
This script provides the configuration and setup needed before creating the agent.

To create the agent in Snowsight:
1. Navigate to AI & ML > Agents > Snowflake Intelligence
2. Click "Create Agent"
3. Use the configuration below
*/

-- ============================================================================
-- STEP 1: VERIFY PREREQUISITES
-- ============================================================================

-- Verify semantic views exist
SELECT 'Semantic Views:' AS check_type;
SHOW SEMANTIC VIEWS IN SCHEMA FGCU_CANVAS_DEMO.ANALYTICS;

-- Verify Cortex Search services exist
SELECT 'Cortex Search Services:' AS check_type;
SHOW CORTEX SEARCH SERVICES IN SCHEMA FGCU_CANVAS_DEMO.ANALYTICS;

-- Verify data exists in curated tables
SELECT 'Data Counts:' AS check_type;
SELECT 
    (SELECT COUNT(*) FROM FGCU_CANVAS_DEMO.CURATED.DIM_STUDENTS) AS students,
    (SELECT COUNT(*) FROM FGCU_CANVAS_DEMO.CURATED.DIM_COURSES) AS courses,
    (SELECT COUNT(*) FROM FGCU_CANVAS_DEMO.CURATED.FACT_ENROLLMENTS) AS enrollments,
    (SELECT COUNT(*) FROM FGCU_CANVAS_DEMO.CURATED.FACT_SUBMISSIONS) AS submissions;


-- ============================================================================
-- STEP 2: CREATE AGENT ROLE AND PERMISSIONS
-- ============================================================================

-- Create a role specifically for the Intelligence agent
CREATE ROLE IF NOT EXISTS CANVAS_INTELLIGENCE_ROLE
    COMMENT = 'Role for FGCU Canvas Intelligence Agent';

-- Grant necessary privileges
GRANT USAGE ON DATABASE FGCU_CANVAS_DEMO TO ROLE CANVAS_INTELLIGENCE_ROLE;
GRANT USAGE ON SCHEMA FGCU_CANVAS_DEMO.ANALYTICS TO ROLE CANVAS_INTELLIGENCE_ROLE;
GRANT USAGE ON SCHEMA FGCU_CANVAS_DEMO.CURATED TO ROLE CANVAS_INTELLIGENCE_ROLE;
GRANT USAGE ON WAREHOUSE FGCU_TRANSFORM_WH TO ROLE CANVAS_INTELLIGENCE_ROLE;

-- Grant access to semantic views
GRANT REFERENCES ON ALL SEMANTIC VIEWS IN SCHEMA FGCU_CANVAS_DEMO.ANALYTICS TO ROLE CANVAS_INTELLIGENCE_ROLE;

-- Grant access to Cortex Search services
GRANT USAGE ON ALL CORTEX SEARCH SERVICES IN SCHEMA FGCU_CANVAS_DEMO.ANALYTICS TO ROLE CANVAS_INTELLIGENCE_ROLE;

-- Grant SELECT on underlying tables
GRANT SELECT ON ALL TABLES IN SCHEMA FGCU_CANVAS_DEMO.CURATED TO ROLE CANVAS_INTELLIGENCE_ROLE;

-- Grant role to ACCOUNTADMIN (or your user role)
GRANT ROLE CANVAS_INTELLIGENCE_ROLE TO ROLE ACCOUNTADMIN;


-- ============================================================================
-- STEP 3: AGENT CONFIGURATION REFERENCE
-- ============================================================================

/*
================================================================================
SNOWFLAKE INTELLIGENCE AGENT CONFIGURATION
================================================================================

Use these settings when creating the agent in Snowsight UI:

AGENT NAME: FGCU Canvas Analytics Assistant

DESCRIPTION:
An AI-powered analytics assistant for FGCU's Canvas Learning Management System.
Ask questions about student performance, course enrollment, grades, assignments,
and learning activity patterns. Get instant visualizations and insights.

MODEL: Auto (recommended) or Claude 3.5

SEMANTIC VIEWS TO CONNECT:
1. FGCU_CANVAS_DEMO.ANALYTICS.CANVAS_STUDENT_ANALYTICS
   - Student demographics and GPA analysis
   
2. FGCU_CANVAS_DEMO.ANALYTICS.CANVAS_COURSE_ANALYTICS
   - Course and department analysis
   
3. FGCU_CANVAS_DEMO.ANALYTICS.CANVAS_ENROLLMENT_ANALYTICS
   - Enrollment patterns and grade distributions
   
4. FGCU_CANVAS_DEMO.ANALYTICS.CANVAS_SUBMISSION_ANALYTICS
   - Assignment submission and grading analysis
   
5. FGCU_CANVAS_DEMO.ANALYTICS.CANVAS_PERFORMANCE_DASHBOARD
   - Aggregated performance metrics

CORTEX SEARCH SERVICES TO CONNECT:
1. FGCU_CANVAS_DEMO.ANALYTICS.CANVAS_ACTIVITY_SEARCH
   - Search student activity logs
   
2. FGCU_CANVAS_DEMO.ANALYTICS.CANVAS_COURSE_SEARCH
   - Search course catalog
   
3. FGCU_CANVAS_DEMO.ANALYTICS.CANVAS_STUDENT_SEARCH
   - Search student directory

WELCOME MESSAGE:
Welcome to the FGCU Canvas Analytics Assistant! 👋

I can help you analyze student performance, course enrollment, grades, and 
learning activity data. Here are some things you can ask me:

📊 **Student Analytics**
- "What is the average GPA by major?"
- "Show students on academic probation"
- "Which majors have the highest performing students?"

📚 **Course Analytics**  
- "List courses by enrollment count"
- "What's the grade distribution for Biology courses?"
- "Which departments have the most courses?"

📈 **Performance Insights**
- "Show me at-risk students with low assignment completion"
- "What's the late submission rate by course?"
- "Compare performance across terms"

What would you like to explore?

================================================================================
*/


-- ============================================================================
-- STEP 4: TEST QUERIES FOR VALIDATION
-- ============================================================================

-- These queries validate the semantic views work correctly
-- Run these before configuring the agent

-- Test 1: Student Analytics
SELECT 
    major,
    total_students,
    average_gpa,
    at_risk_students,
    deans_list_students
FROM CANVAS_STUDENT_ANALYTICS
GROUP BY major
ORDER BY total_students DESC
LIMIT 10;

-- Test 2: Course Analytics
SELECT 
    department,
    total_courses,
    total_credit_hours,
    unique_instructors
FROM CANVAS_COURSE_ANALYTICS
GROUP BY department
ORDER BY total_courses DESC
LIMIT 10;

-- Test 3: Enrollment Analytics
SELECT 
    term,
    department,
    total_enrollments,
    unique_students,
    average_grade_points,
    completion_rate,
    pass_rate
FROM CANVAS_ENROLLMENT_ANALYTICS
GROUP BY term, department
ORDER BY term DESC, total_enrollments DESC
LIMIT 20;

-- Test 4: Submission Analytics
SELECT 
    assignment_type,
    total_submissions,
    average_percentage,
    late_submission_rate
FROM CANVAS_SUBMISSION_ANALYTICS
GROUP BY assignment_type
ORDER BY total_submissions DESC;

-- Test 5: Performance Dashboard
SELECT 
    department,
    term,
    COUNT(*) AS student_course_count,
    avg_completion_rate,
    avg_course_score,
    students_at_risk,
    high_performers
FROM CANVAS_PERFORMANCE_DASHBOARD
GROUP BY department, term
ORDER BY term DESC, student_course_count DESC
LIMIT 20;


-- ============================================================================
-- STEP 5: SAMPLE CORTEX ANALYST QUERIES
-- ============================================================================

/*
These demonstrate how Cortex Analyst will query the semantic views.
The agent generates SQL like this based on natural language questions.
*/

-- Question: "What is the average GPA by major?"
SELECT 
    major AS "Major",
    ROUND(AVG(student_gpa), 2) AS "Average GPA",
    COUNT(*) AS "Student Count"
FROM CANVAS_STUDENT_ANALYTICS
GROUP BY major
ORDER BY "Average GPA" DESC;

-- Question: "Which students are at risk?"
SELECT 
    student_name AS "Student",
    major AS "Major",
    classification AS "Classification",
    ROUND(student_gpa, 2) AS "GPA",
    academic_standing AS "Standing"
FROM CANVAS_STUDENT_ANALYTICS
WHERE academic_standing = 'Academic Probation'
ORDER BY student_gpa ASC
LIMIT 25;

-- Question: "Show enrollment trends by term"
SELECT 
    term AS "Term",
    COUNT(DISTINCT student_id) AS "Unique Students",
    COUNT(*) AS "Total Enrollments",
    ROUND(AVG(grade_point), 2) AS "Avg Grade Points"
FROM CANVAS_ENROLLMENT_ANALYTICS
GROUP BY term
ORDER BY term;

-- Question: "What's the late submission rate by assignment type?"
SELECT 
    assignment_type AS "Assignment Type",
    total_submissions AS "Total",
    late_submission_count AS "Late",
    ROUND(late_submission_rate, 1) || '%' AS "Late Rate"
FROM CANVAS_SUBMISSION_ANALYTICS
GROUP BY assignment_type
ORDER BY late_submission_rate DESC;


-- ============================================================================
-- STEP 6: CREATE AGENT VIA SNOWSIGHT UI
-- ============================================================================

/*
INSTRUCTIONS TO CREATE THE AGENT:

1. Open Snowsight and navigate to: AI & ML > Agents

2. Click on the "Snowflake Intelligence" tab

3. Click "Create Agent" button

4. Fill in the configuration:
   
   GENERAL SETTINGS:
   - Name: FGCU_CANVAS_ANALYTICS_AGENT
   - Description: AI assistant for FGCU Canvas LMS analytics
   - Model: Auto (or Claude 3.5)
   
   DATA SOURCES:
   - Add Semantic Views:
     ✓ FGCU_CANVAS_DEMO.ANALYTICS.CANVAS_STUDENT_ANALYTICS
     ✓ FGCU_CANVAS_DEMO.ANALYTICS.CANVAS_COURSE_ANALYTICS
     ✓ FGCU_CANVAS_DEMO.ANALYTICS.CANVAS_ENROLLMENT_ANALYTICS
     ✓ FGCU_CANVAS_DEMO.ANALYTICS.CANVAS_SUBMISSION_ANALYTICS
     ✓ FGCU_CANVAS_DEMO.ANALYTICS.CANVAS_PERFORMANCE_DASHBOARD
   
   - Add Cortex Search Services:
     ✓ FGCU_CANVAS_DEMO.ANALYTICS.CANVAS_ACTIVITY_SEARCH
     ✓ FGCU_CANVAS_DEMO.ANALYTICS.CANVAS_COURSE_SEARCH
     ✓ FGCU_CANVAS_DEMO.ANALYTICS.CANVAS_STUDENT_SEARCH

5. Configure appearance (optional):
   - Brand name: FGCU Canvas Analytics
   - Welcome message: (use the one provided above)
   - Color theme: #007749 (FGCU Green)

6. Set visibility:
   - Make visible to roles that need access

7. Click "Create" to finalize

8. Test the agent with sample questions

*/


-- ============================================================================
-- VERIFICATION & TROUBLESHOOTING
-- ============================================================================

-- Check all objects are created
SELECT 'Verification Summary' AS status;

SELECT 'Semantic Views' AS object_type, COUNT(*) AS count 
FROM TABLE(RESULT_SCAN(LAST_QUERY_ID(-2)));

SELECT 'Search Services' AS object_type, COUNT(*) AS count
FROM TABLE(RESULT_SCAN(LAST_QUERY_ID(-3)));

-- Verify role permissions
SHOW GRANTS TO ROLE CANVAS_INTELLIGENCE_ROLE;

-- Check if semantic views are queryable
SELECT 'Testing CANVAS_STUDENT_ANALYTICS...' AS test;
SELECT COUNT(*) AS row_count FROM CANVAS_STUDENT_ANALYTICS LIMIT 1;

SELECT 'Testing CANVAS_COURSE_ANALYTICS...' AS test;
SELECT COUNT(*) AS row_count FROM CANVAS_COURSE_ANALYTICS LIMIT 1;

SELECT 'Testing CANVAS_ENROLLMENT_ANALYTICS...' AS test;
SELECT COUNT(*) AS row_count FROM CANVAS_ENROLLMENT_ANALYTICS LIMIT 1;


PRINT '================================================================================';
PRINT '✅ Intelligence Agent setup complete!';
PRINT '';
PRINT 'Next steps:';
PRINT '1. Go to Snowsight > AI & ML > Agents > Snowflake Intelligence';
PRINT '2. Click "Create Agent"';
PRINT '3. Configure using the settings in this script';
PRINT '4. Test with sample questions';
PRINT '================================================================================';

