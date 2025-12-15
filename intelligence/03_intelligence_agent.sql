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
SELECT 'Checking Semantic Views...' AS status;
SHOW SEMANTIC VIEWS IN SCHEMA FGCU_CANVAS_DEMO.ANALYTICS;

-- Verify Cortex Search services exist
SELECT 'Checking Cortex Search Services...' AS status;
SHOW CORTEX SEARCH SERVICES IN SCHEMA FGCU_CANVAS_DEMO.ANALYTICS;

-- Verify base views exist
SELECT 'Checking Base Views...' AS status;
SHOW VIEWS LIKE 'VW_%' IN SCHEMA FGCU_CANVAS_DEMO.ANALYTICS;

-- Verify data exists in curated tables
SELECT 'Checking Data Counts...' AS status;
SELECT 
    (SELECT COUNT(*) FROM FGCU_CANVAS_DEMO.CURATED.DIM_STUDENTS) AS students,
    (SELECT COUNT(*) FROM FGCU_CANVAS_DEMO.CURATED.DIM_COURSES) AS courses,
    (SELECT COUNT(*) FROM FGCU_CANVAS_DEMO.CURATED.FACT_ENROLLMENTS) AS enrollments,
    (SELECT COUNT(*) FROM FGCU_CANVAS_DEMO.CURATED.FACT_SUBMISSIONS) AS submissions,
    (SELECT COUNT(*) FROM FGCU_CANVAS_DEMO.CURATED.FACT_ACTIVITY_LOGS) AS activity_logs;


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

-- Grant access to semantic views (REFERENCES + SELECT needed for Cortex Analyst)
GRANT REFERENCES, SELECT ON ALL SEMANTIC VIEWS IN SCHEMA FGCU_CANVAS_DEMO.ANALYTICS TO ROLE CANVAS_INTELLIGENCE_ROLE;

-- Grant access to base views
GRANT SELECT ON ALL VIEWS IN SCHEMA FGCU_CANVAS_DEMO.ANALYTICS TO ROLE CANVAS_INTELLIGENCE_ROLE;

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
   - Dimensions: student_name, major, classification, academic_standing
   - Metrics: total_students, average_gpa, at_risk_students, deans_list_students
   
2. FGCU_CANVAS_DEMO.ANALYTICS.CANVAS_COURSE_ANALYTICS
   - Dimensions: course_code, course_name, department, term, instructor
   - Metrics: total_courses, total_credit_hours, unique_instructors
   
3. FGCU_CANVAS_DEMO.ANALYTICS.CANVAS_ENROLLMENT_ANALYTICS
   - Dimensions: student_name, course_name, term, grade_category
   - Metrics: total_enrollments, unique_students, average_grade_points
   
4. FGCU_CANVAS_DEMO.ANALYTICS.CANVAS_SUBMISSION_ANALYTICS
   - Dimensions: assignment_name, assignment_type, student_name, score_category
   - Metrics: total_submissions, average_score, late_submission_count
   
5. FGCU_CANVAS_DEMO.ANALYTICS.CANVAS_PERFORMANCE_DASHBOARD
   - Dimensions: student_name, course_name, term, department
   - Metrics: avg_course_score, students_at_risk, high_performers

CORTEX SEARCH SERVICES TO CONNECT:
1. FGCU_CANVAS_DEMO.ANALYTICS.CANVAS_ACTIVITY_SEARCH
   - Search student activity logs by type, page, device
   
2. FGCU_CANVAS_DEMO.ANALYTICS.CANVAS_COURSE_SEARCH
   - Search course catalog by name, department, instructor
   
3. FGCU_CANVAS_DEMO.ANALYTICS.CANVAS_STUDENT_SEARCH
   - Search student directory by name, major, status

WELCOME MESSAGE:
Welcome to the FGCU Canvas Analytics Assistant! 👋

I can help you analyze student performance, course enrollment, grades, and 
learning activity data. Here are some things you can ask me:

📊 **Student Analytics**
- "What is the average GPA by major?"
- "Show students on academic probation"
- "Which majors have the highest performing students?"

📚 **Course Analytics**  
- "List courses by department"
- "How many credit hours does each department offer?"
- "Which instructors teach the most courses?"

📈 **Performance Insights**
- "Show me at-risk students"
- "What's the late submission count by assignment type?"
- "Compare student performance across terms"

What would you like to explore?

================================================================================
*/


-- ============================================================================
-- STEP 4: TEST BASE VIEWS (More reliable than semantic view queries)
-- ============================================================================

-- Test 1: Student data via base view
SELECT 'Testing VW_STUDENTS_BASE...' AS test;
SELECT 
    major,
    COUNT(*) AS student_count,
    ROUND(AVG(gpa), 2) AS avg_gpa,
    SUM(CASE WHEN gpa < 2.0 THEN 1 ELSE 0 END) AS at_risk_count,
    SUM(CASE WHEN gpa >= 3.5 THEN 1 ELSE 0 END) AS deans_list_count
FROM VW_STUDENTS_BASE
GROUP BY major
ORDER BY student_count DESC
LIMIT 10;

-- Test 2: Course data via base view
SELECT 'Testing VW_COURSES_BASE...' AS test;
SELECT 
    department,
    COUNT(*) AS course_count,
    SUM(credit_hours) AS total_credits,
    COUNT(DISTINCT instructor_name) AS instructor_count
FROM VW_COURSES_BASE
GROUP BY department
ORDER BY course_count DESC
LIMIT 10;

-- Test 3: Enrollment data via base view
SELECT 'Testing VW_ENROLLMENTS_BASE...' AS test;
SELECT 
    term,
    department,
    COUNT(*) AS enrollment_count,
    COUNT(DISTINCT student_id) AS unique_students,
    ROUND(AVG(grade_points), 2) AS avg_grade_points
FROM VW_ENROLLMENTS_BASE
WHERE term IS NOT NULL
GROUP BY term, department
ORDER BY term DESC, enrollment_count DESC
LIMIT 20;

-- Test 4: Submission data via base view
SELECT 'Testing VW_SUBMISSIONS_BASE...' AS test;
SELECT 
    assignment_type,
    COUNT(*) AS submission_count,
    ROUND(AVG(percentage), 1) AS avg_percentage,
    SUM(CASE WHEN late_flag THEN 1 ELSE 0 END) AS late_count
FROM VW_SUBMISSIONS_BASE
WHERE assignment_type IS NOT NULL
GROUP BY assignment_type
ORDER BY submission_count DESC;

-- Test 5: Performance data via base view
SELECT 'Testing VW_PERFORMANCE_BASE...' AS test;
SELECT 
    department,
    term,
    COUNT(*) AS record_count,
    ROUND(AVG(average_score), 1) AS avg_score,
    SUM(CASE WHEN average_score < 60 THEN 1 ELSE 0 END) AS at_risk,
    SUM(CASE WHEN average_score >= 90 THEN 1 ELSE 0 END) AS high_performers
FROM VW_PERFORMANCE_BASE
WHERE term IS NOT NULL
GROUP BY department, term
ORDER BY term DESC, record_count DESC
LIMIT 20;


-- ============================================================================
-- STEP 5: VERIFY SEMANTIC VIEWS ARE QUERYABLE
-- ============================================================================

-- These simple queries test that semantic views are accessible
SELECT 'Testing semantic view access...' AS status;

-- Student Analytics
SELECT COUNT(*) AS student_rows FROM CANVAS_STUDENT_ANALYTICS;

-- Course Analytics
SELECT COUNT(*) AS course_rows FROM CANVAS_COURSE_ANALYTICS;

-- Enrollment Analytics
SELECT COUNT(*) AS enrollment_rows FROM CANVAS_ENROLLMENT_ANALYTICS;

-- Submission Analytics
SELECT COUNT(*) AS submission_rows FROM CANVAS_SUBMISSION_ANALYTICS;

-- Performance Dashboard
SELECT COUNT(*) AS performance_rows FROM CANVAS_PERFORMANCE_DASHBOARD;


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
-- VERIFICATION SUMMARY
-- ============================================================================

-- Final verification
SELECT 'Setup Verification Summary' AS status;

-- Count semantic views
SELECT 'Semantic Views Created' AS check_item, 
       (SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES 
        WHERE TABLE_SCHEMA = 'ANALYTICS' AND TABLE_NAME LIKE 'CANVAS_%') AS count;

-- Count base views  
SELECT 'Base Views Created' AS check_item,
       (SELECT COUNT(*) FROM INFORMATION_SCHEMA.VIEWS 
        WHERE TABLE_SCHEMA = 'ANALYTICS' AND TABLE_NAME LIKE 'VW_%') AS count;

-- Verify role exists
SHOW ROLES LIKE 'CANVAS_INTELLIGENCE_ROLE';

SELECT '✅ Intelligence Agent setup complete!' AS status;
SELECT 'Next: Go to Snowsight > AI & ML > Agents > Snowflake Intelligence' AS next_step;
