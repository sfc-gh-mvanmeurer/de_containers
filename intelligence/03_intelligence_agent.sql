/*
================================================================================
Demo Canvas Analytics - Snowflake Intelligence Agent Setup
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
USE DATABASE DEMO_CANVAS_DB;
USE WAREHOUSE DEMO_TRANSFORM_WH;
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
SHOW SEMANTIC VIEWS IN SCHEMA DEMO_CANVAS_DB.ANALYTICS;

-- Verify Cortex Search services exist
SELECT 'Checking Cortex Search Services...' AS status;
SHOW CORTEX SEARCH SERVICES IN SCHEMA DEMO_CANVAS_DB.ANALYTICS;

-- Verify base views exist
SELECT 'Checking Base Views...' AS status;
SHOW VIEWS LIKE 'VW_%' IN SCHEMA DEMO_CANVAS_DB.ANALYTICS;

-- Verify data exists in curated tables
SELECT 'Checking Data Counts...' AS status;
SELECT 
    (SELECT COUNT(*) FROM DEMO_CANVAS_DB.CURATED.DIM_STUDENTS) AS students,
    (SELECT COUNT(*) FROM DEMO_CANVAS_DB.CURATED.DIM_COURSES) AS courses,
    (SELECT COUNT(*) FROM DEMO_CANVAS_DB.CURATED.FACT_ENROLLMENTS) AS enrollments,
    (SELECT COUNT(*) FROM DEMO_CANVAS_DB.CURATED.FACT_SUBMISSIONS) AS submissions,
    (SELECT COUNT(*) FROM DEMO_CANVAS_DB.CURATED.FACT_ACTIVITY_LOGS) AS activity_logs;


-- ============================================================================
-- STEP 2: CREATE AGENT ROLE AND PERMISSIONS
-- ============================================================================

-- Create a role specifically for the Intelligence agent
CREATE ROLE IF NOT EXISTS CANVAS_INTELLIGENCE_ROLE
    COMMENT = 'Role for Demo Canvas Intelligence Agent';

-- Grant necessary privileges
GRANT USAGE ON DATABASE DEMO_CANVAS_DB TO ROLE CANVAS_INTELLIGENCE_ROLE;
GRANT USAGE ON SCHEMA DEMO_CANVAS_DB.ANALYTICS TO ROLE CANVAS_INTELLIGENCE_ROLE;
GRANT USAGE ON SCHEMA DEMO_CANVAS_DB.CURATED TO ROLE CANVAS_INTELLIGENCE_ROLE;
GRANT USAGE ON WAREHOUSE DEMO_TRANSFORM_WH TO ROLE CANVAS_INTELLIGENCE_ROLE;

-- Grant access to semantic views (REFERENCES + SELECT needed for Cortex Analyst)
GRANT REFERENCES, SELECT ON ALL SEMANTIC VIEWS IN SCHEMA DEMO_CANVAS_DB.ANALYTICS TO ROLE CANVAS_INTELLIGENCE_ROLE;

-- Grant access to base views
GRANT SELECT ON ALL VIEWS IN SCHEMA DEMO_CANVAS_DB.ANALYTICS TO ROLE CANVAS_INTELLIGENCE_ROLE;

-- Grant access to Cortex Search services
GRANT USAGE ON ALL CORTEX SEARCH SERVICES IN SCHEMA DEMO_CANVAS_DB.ANALYTICS TO ROLE CANVAS_INTELLIGENCE_ROLE;

-- Grant SELECT on underlying tables
GRANT SELECT ON ALL TABLES IN SCHEMA DEMO_CANVAS_DB.CURATED TO ROLE CANVAS_INTELLIGENCE_ROLE;

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

AGENT NAME: Demo Canvas Analytics Assistant

DESCRIPTION:
An AI-powered analytics assistant for Demo's Canvas Learning Management System.
Ask questions about student performance, course enrollment, grades, assignments,
and learning activity patterns. Get instant visualizations and insights.

MODEL: Auto (recommended) or Claude 3.5

SEMANTIC VIEWS TO CONNECT:
1. DEMO_CANVAS_DB.ANALYTICS.CANVAS_STUDENT_ANALYTICS
   - Dimensions: student_name, major, classification, academic_standing
   - Metrics: total_students, average_gpa, at_risk_students, deans_list_students
   
2. DEMO_CANVAS_DB.ANALYTICS.CANVAS_COURSE_ANALYTICS
   - Dimensions: course_code, course_name, department, term, instructor
   - Metrics: total_courses, total_credit_hours, unique_instructors
   
3. DEMO_CANVAS_DB.ANALYTICS.CANVAS_ENROLLMENT_ANALYTICS
   - Dimensions: student_name, course_name, term, grade_category
   - Metrics: total_enrollments, unique_students, average_grade_points
   
4. DEMO_CANVAS_DB.ANALYTICS.CANVAS_SUBMISSION_ANALYTICS
   - Dimensions: assignment_name, assignment_type, student_name, score_category
   - Metrics: total_submissions, average_score, late_submission_count
   
5. DEMO_CANVAS_DB.ANALYTICS.CANVAS_PERFORMANCE_DASHBOARD
   - Dimensions: student_name, course_name, term, department
   - Metrics: avg_course_score, students_at_risk, high_performers

CORTEX SEARCH SERVICES TO CONNECT:
1. DEMO_CANVAS_DB.ANALYTICS.CANVAS_ACTIVITY_SEARCH
   - Search student activity logs by type, page, device
   
2. DEMO_CANVAS_DB.ANALYTICS.CANVAS_COURSE_SEARCH
   - Search course catalog by name, department, instructor
   
3. DEMO_CANVAS_DB.ANALYTICS.CANVAS_STUDENT_SEARCH
   - Search student directory by name, major, status

WELCOME MESSAGE:
Welcome to the Demo Canvas Analytics Assistant! 👋

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
-- STEP 5: VERIFY SEMANTIC VIEWS EXIST
-- ============================================================================

/*
NOTE: Direct SELECT from semantic views may require enabling the feature:
  
  -- Check if semantic views feature is enabled
  SELECT SYSTEM$BEHAVIOR_CHANGE_BUNDLE_STATUS('2024_08');
  
  -- If needed, enable semantic views (requires ACCOUNTADMIN)
  ALTER ACCOUNT SET ENABLE_SEMANTIC_VIEWS = TRUE;

Semantic views are primarily used by Cortex Analyst, not direct SQL queries.
The agent will use them through the cortex_analyst_text_to_sql tool type.
*/

-- Verify semantic views were created (metadata check, not data query)
SELECT 'Checking semantic views exist...' AS status;
SHOW SEMANTIC VIEWS IN SCHEMA DEMO_CANVAS_DB.ANALYTICS;

-- Describe each semantic view to verify structure
DESCRIBE SEMANTIC VIEW CANVAS_STUDENT_ANALYTICS;
DESCRIBE SEMANTIC VIEW CANVAS_COURSE_ANALYTICS;
DESCRIBE SEMANTIC VIEW CANVAS_ENROLLMENT_ANALYTICS;
DESCRIBE SEMANTIC VIEW CANVAS_SUBMISSION_ANALYTICS;
DESCRIBE SEMANTIC VIEW CANVAS_PERFORMANCE_DASHBOARD;

-- Verify base views are queryable (these always work)
SELECT 'Testing base views...' AS status;
SELECT COUNT(*) AS student_count FROM VW_STUDENTS_BASE;
SELECT COUNT(*) AS course_count FROM VW_COURSES_BASE;
SELECT COUNT(*) AS enrollment_count FROM VW_ENROLLMENTS_BASE;
SELECT COUNT(*) AS submission_count FROM VW_SUBMISSIONS_BASE;
SELECT COUNT(*) AS performance_count FROM VW_PERFORMANCE_BASE;


-- ============================================================================
-- STEP 6: CREATE AGENT VIA SQL
-- Reference: https://docs.snowflake.com/en/user-guide/snowflake-cortex/cortex-agents-manage
-- ============================================================================

-- Create the Cortex Agent with semantic views and search services
CREATE OR REPLACE AGENT DEMO_CANVAS_DB.ANALYTICS.CANVAS_ANALYTICS_AGENT
    COMMENT = 'AI-powered analytics assistant for Demo Canvas LMS data'
    PROFILE = '{
        "display_name": "Demo Canvas Analytics Assistant",
        "avatar": "📊",
        "color": "#007749"
    }'
    FROM SPECIFICATION
$$
models:
  orchestration: claude-4-sonnet

orchestration:
  budget:
    seconds: 60
    tokens: 32000

instructions:
  system: |
    You are the Demo Canvas Analytics Assistant, an AI-powered data analyst for 
    Demo University's Canvas Learning Management System.
    
    You help faculty, advisors, and administrators analyze:
    - Student performance and demographics
    - Course enrollment and grades  
    - Assignment submissions and completion rates
    - Learning activity patterns and engagement
    
    Always be helpful, accurate, and provide actionable insights.

  orchestration: |
    Tool Selection Guidelines:
    
    1. For STRUCTURED DATA queries (counts, averages, comparisons):
       - Use StudentAnalyst for student demographics, GPA, majors, classification
       - Use CourseAnalyst for course catalog, departments, instructors, credits
       - Use EnrollmentAnalyst for enrollment data, grades, student-course relationships
       - Use SubmissionAnalyst for assignment submissions, scores, late work
       - Use PerformanceAnalyst for aggregated student-course performance metrics
    
    2. For SEARCH queries (find specific students, courses, activities):
       - Use StudentSearch for finding students by name, major, or status
       - Use CourseSearch for finding courses by name, department, or instructor
       - Use ActivitySearch for finding specific activity patterns
    
    3. For VISUALIZATION requests:
       - Use data_to_chart to generate charts from query results
    
    Multi-tool coordination:
    - For questions spanning students AND courses, query both and combine insights
    - For trend analysis, use appropriate analyst tool then visualize with chart

  response: |
    Response Guidelines:
    - Lead with the direct answer to the question
    - Include relevant numbers and statistics
    - Provide context when helpful (e.g., "This is above/below average")
    - Offer follow-up suggestions when appropriate
    - Use clear formatting with headers and bullet points for complex responses
    - When showing data, consider if a visualization would help

  sample_questions:
    - question: "What is the average GPA by major?"
      answer: "I'll analyze student GPA data grouped by major using the Student Analytics tool."
    - question: "Show me students at risk of failing"
      answer: "I'll identify students with GPA below 2.0 or those on academic probation."
    - question: "Which courses have the highest enrollment?"
      answer: "I'll query enrollment data and rank courses by student count."
    - question: "What's the late submission rate by assignment type?"
      answer: "I'll analyze submission data to calculate late rates for each assignment type."

tools:
  # Cortex Analyst tools for structured data
  - tool_spec:
      type: cortex_analyst_text_to_sql
      name: StudentAnalyst
      description: "Analyzes student demographics, GPA, majors, classification, and academic standing. Use for questions about student counts, average GPA, at-risk students, and dean's list."
  
  - tool_spec:
      type: cortex_analyst_text_to_sql
      name: CourseAnalyst
      description: "Analyzes course catalog including departments, instructors, credit hours, and delivery modes. Use for questions about course offerings and curriculum."
  
  - tool_spec:
      type: cortex_analyst_text_to_sql
      name: EnrollmentAnalyst
      description: "Analyzes enrollment patterns, grades, and student-course relationships. Use for questions about enrollment counts, grade distributions, and pass rates."
  
  - tool_spec:
      type: cortex_analyst_text_to_sql
      name: SubmissionAnalyst
      description: "Analyzes assignment submissions, scores, and grading patterns. Use for questions about submission rates, late work, and assignment performance."
  
  - tool_spec:
      type: cortex_analyst_text_to_sql
      name: PerformanceAnalyst
      description: "Analyzes aggregated student-course performance metrics. Use for questions about completion rates, at-risk students, and high performers."

  # Cortex Search tools for unstructured search
  - tool_spec:
      type: cortex_search
      name: StudentSearch
      description: "Search student directory by name, major, classification, or status. Use when user wants to find specific students."
  
  - tool_spec:
      type: cortex_search
      name: CourseSearch
      description: "Search course catalog by name, department, instructor, or delivery mode. Use when user wants to find specific courses."
  
  - tool_spec:
      type: cortex_search
      name: ActivitySearch
      description: "Search student activity logs by type, page, or device. Use when user wants to find specific learning activities."

  # Visualization tool
  - tool_spec:
      type: data_to_chart
      name: data_to_chart
      description: "Generate visualizations from data. Use when query results would benefit from a chart or graph."

tool_resources:
  StudentAnalyst:
    semantic_view: DEMO_CANVAS_DB.ANALYTICS.CANVAS_STUDENT_ANALYTICS
  
  CourseAnalyst:
    semantic_view: DEMO_CANVAS_DB.ANALYTICS.CANVAS_COURSE_ANALYTICS
  
  EnrollmentAnalyst:
    semantic_view: DEMO_CANVAS_DB.ANALYTICS.CANVAS_ENROLLMENT_ANALYTICS
  
  SubmissionAnalyst:
    semantic_view: DEMO_CANVAS_DB.ANALYTICS.CANVAS_SUBMISSION_ANALYTICS
  
  PerformanceAnalyst:
    semantic_view: DEMO_CANVAS_DB.ANALYTICS.CANVAS_PERFORMANCE_DASHBOARD
  
  StudentSearch:
    name: DEMO_CANVAS_DB.ANALYTICS.CANVAS_STUDENT_SEARCH
    max_results: 10
    title_column: full_name
    id_column: student_id
  
  CourseSearch:
    name: DEMO_CANVAS_DB.ANALYTICS.CANVAS_COURSE_SEARCH
    max_results: 10
    title_column: course_name
    id_column: course_id
  
  ActivitySearch:
    name: DEMO_CANVAS_DB.ANALYTICS.CANVAS_ACTIVITY_SEARCH
    max_results: 10
    title_column: activity_type
    id_column: activity_id
$$;

-- Grant usage on the agent to appropriate roles
GRANT USAGE ON AGENT DEMO_CANVAS_DB.ANALYTICS.CANVAS_ANALYTICS_AGENT TO ROLE CANVAS_INTELLIGENCE_ROLE;
GRANT USAGE ON AGENT DEMO_CANVAS_DB.ANALYTICS.CANVAS_ANALYTICS_AGENT TO ROLE ACCOUNTADMIN;

-- Verify the agent was created
SHOW AGENTS IN SCHEMA DEMO_CANVAS_DB.ANALYTICS;
DESCRIBE AGENT DEMO_CANVAS_DB.ANALYTICS.CANVAS_ANALYTICS_AGENT;


-- ============================================================================
-- ALTERNATIVE: CREATE AGENT VIA SNOWSIGHT UI
-- ============================================================================

/*
If you prefer to create the agent via the UI instead of SQL:

1. Open Snowsight and navigate to: AI & ML > Agents
2. Click "Create Agent" button
3. Configure using the same settings as the SQL specification above
4. Add the semantic views and search services as tools
5. Test with sample questions
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
