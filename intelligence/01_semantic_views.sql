/*
================================================================================
Demo Canvas Analytics - Semantic Views Setup
================================================================================
Creates semantic views on top of the curated Canvas LMS data to enable 
natural language queries through Cortex Analyst and Snowflake Intelligence.

Reference: https://docs.snowflake.com/en/user-guide/views-semantic/sql
================================================================================
*/

-- ============================================================================
-- SETUP
-- ============================================================================
USE ROLE ACCOUNTADMIN;
USE DATABASE DEMO_CANVAS_DB;
USE WAREHOUSE DEMO_TRANSFORM_WH;

-- Create analytics schema for semantic views
CREATE SCHEMA IF NOT EXISTS ANALYTICS
    COMMENT = 'Schema for semantic views and analytics objects';

USE SCHEMA ANALYTICS;

-- ============================================================================
-- STEP 1: CREATE BASE VIEWS WITH COMPUTED COLUMNS
-- ============================================================================

-- Base view for students with computed columns
CREATE OR REPLACE VIEW VW_STUDENTS_BASE AS
SELECT 
    student_id,
    canvas_user_id,
    first_name,
    last_name,
    CONCAT(first_name, ' ', last_name) AS full_name,
    email,
    major,
    classification,
    enrollment_status,
    enrollment_date,
    expected_graduation,
    gpa,
    advisor_id,
    CASE 
        WHEN gpa >= 3.5 THEN 'Deans List'
        WHEN gpa >= 3.0 THEN 'Good Standing'
        WHEN gpa >= 2.0 THEN 'Satisfactory'
        ELSE 'Academic Probation'
    END AS academic_standing,
    created_at,
    updated_at
FROM DEMO_CANVAS_DB.CURATED.DIM_STUDENTS;

-- Base view for courses with computed columns
CREATE OR REPLACE VIEW VW_COURSES_BASE AS
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
    instructor_id,
    instructor_name,
    start_date,
    end_date,
    max_enrollment,
    is_current,
    created_at
FROM DEMO_CANVAS_DB.CURATED.DIM_COURSES;

-- Base view for enrollments with computed columns
CREATE OR REPLACE VIEW VW_ENROLLMENTS_BASE AS
SELECT 
    e.enrollment_id,
    e.student_id,
    e.course_id,
    e.enrolled_at AS enrollment_date,
    e.enrollment_state AS enrollment_status,
    e.final_grade,
    e.final_score,
    CASE 
        WHEN e.final_score >= 90 THEN 4.0
        WHEN e.final_score >= 80 THEN 3.0
        WHEN e.final_score >= 70 THEN 2.0
        WHEN e.final_score >= 60 THEN 1.0
        WHEN e.final_score IS NOT NULL THEN 0.0
        ELSE NULL
    END AS grade_points,
    CASE 
        WHEN e.final_score >= 90 THEN 'Excellent'
        WHEN e.final_score >= 80 THEN 'Good'
        WHEN e.final_score >= 70 THEN 'Satisfactory'
        WHEN e.final_score >= 60 THEN 'Poor'
        WHEN e.final_score IS NOT NULL THEN 'Failing'
        ELSE 'Not Graded'
    END AS grade_category,
    s.first_name AS student_first_name,
    s.last_name AS student_last_name,
    CONCAT(s.first_name, ' ', s.last_name) AS student_name,
    s.major AS student_major,
    s.classification AS student_classification,
    c.course_code,
    c.course_name,
    c.department,
    c.term,
    c.instructor_name,
    c.credit_hours,
    e.created_at
FROM DEMO_CANVAS_DB.CURATED.FACT_ENROLLMENTS e
LEFT JOIN DEMO_CANVAS_DB.CURATED.DIM_STUDENTS s ON e.student_id = s.student_id
LEFT JOIN DEMO_CANVAS_DB.CURATED.DIM_COURSES c ON e.course_id = c.course_id;

-- Base view for submissions with computed columns
CREATE OR REPLACE VIEW VW_SUBMISSIONS_BASE AS
SELECT 
    sub.submission_id,
    sub.assignment_id,
    sub.student_id,
    sub.submitted_at AS submission_date,
    sub.score,
    sub.grade,
    sub.late_flag,
    sub.attempt_number,
    sub.graded_at,
    sub.grader_id,
    sub.percentage,
    a.assignment_name,
    a.assignment_type,
    a.points_possible,
    a.due_date,
    a.course_id,
    CASE 
        WHEN sub.percentage >= 90 THEN 'Excellent'
        WHEN sub.percentage >= 80 THEN 'Good'
        WHEN sub.percentage >= 70 THEN 'Satisfactory'
        WHEN sub.percentage >= 60 THEN 'Below Average'
        ELSE 'Failing'
    END AS score_category,
    s.first_name AS student_first_name,
    s.last_name AS student_last_name,
    CONCAT(s.first_name, ' ', s.last_name) AS student_name,
    s.major AS student_major,
    sub.created_at
FROM DEMO_CANVAS_DB.CURATED.FACT_SUBMISSIONS sub
LEFT JOIN DEMO_CANVAS_DB.CURATED.DIM_ASSIGNMENTS a ON sub.assignment_id = a.assignment_id
LEFT JOIN DEMO_CANVAS_DB.CURATED.DIM_STUDENTS s ON sub.student_id = s.student_id;

-- Base view for performance aggregates
CREATE OR REPLACE VIEW VW_PERFORMANCE_BASE AS
SELECT 
    p.student_id,
    p.course_id,
    p.term,
    p.total_assignments,
    p.completed_assignments,
    p.avg_score AS average_score,
    p.total_points_earned,
    p.total_points_possible,
    p.late_submissions,
    p.missing_submissions,
    p.total_activity_minutes,
    p.current_grade AS performance_grade,
    p.last_activity_date,
    p.calculated_at,
    CASE 
        WHEN p.total_assignments > 0 
        THEN ROUND(p.completed_assignments * 100.0 / p.total_assignments, 1)
        ELSE 0
    END AS completion_rate,
    s.first_name AS student_first_name,
    s.last_name AS student_last_name,
    CONCAT(s.first_name, ' ', s.last_name) AS student_name,
    s.major,
    s.classification,
    s.gpa AS overall_gpa,
    c.course_code,
    c.course_name,
    c.department,
    c.instructor_name
FROM DEMO_CANVAS_DB.CURATED.AGG_STUDENT_COURSE_PERFORMANCE p
LEFT JOIN DEMO_CANVAS_DB.CURATED.DIM_STUDENTS s ON p.student_id = s.student_id
LEFT JOIN DEMO_CANVAS_DB.CURATED.DIM_COURSES c ON p.course_id = c.course_id;


-- ============================================================================
-- STEP 2: CREATE SEMANTIC VIEWS
-- Using correct syntax: table.name AS expression COMMENT = 'description'
-- ============================================================================

-- Semantic View 1: Student Analytics
CREATE OR REPLACE SEMANTIC VIEW CANVAS_STUDENT_ANALYTICS

  TABLES (
    students AS DEMO_CANVAS_DB.ANALYTICS.VW_STUDENTS_BASE
      PRIMARY KEY (student_id)
      COMMENT = 'Student dimension with demographics and GPA'
  )

  FACTS (
    students.student_gpa AS gpa
      COMMENT = 'Individual student GPA on 4.0 scale'
  )

  DIMENSIONS (
    students.student_id AS student_id
      COMMENT = 'Unique student identifier',
    students.student_name AS full_name
      COMMENT = 'Full name of the student',
    students.first_name AS first_name
      COMMENT = 'Student first name',
    students.last_name AS last_name
      COMMENT = 'Student last name',
    students.email AS email
      COMMENT = 'Student email address',
    students.major AS major
      COMMENT = 'Student academic major',
    students.classification AS classification
      COMMENT = 'Student year: Freshman, Sophomore, Junior, Senior',
    students.enrollment_status AS enrollment_status
      COMMENT = 'Current enrollment status',
    students.enrollment_date AS enrollment_date
      COMMENT = 'Date student first enrolled',
    students.graduation_date AS expected_graduation
      COMMENT = 'Expected graduation date',
    students.academic_standing AS academic_standing
      COMMENT = 'Academic standing based on GPA'
  )

  METRICS (
    students.total_students AS COUNT(student_id)
      COMMENT = 'Total number of students',
    students.average_gpa AS AVG(gpa)
      COMMENT = 'Average GPA across students',
    students.active_students AS COUNT_IF(enrollment_status = 'Active')
      COMMENT = 'Number of currently active students',
    students.at_risk_students AS COUNT_IF(gpa < 2.0)
      COMMENT = 'Students with GPA below 2.0',
    students.deans_list_students AS COUNT_IF(gpa >= 3.5)
      COMMENT = 'Students on Deans List with GPA >= 3.5'
  )

  COMMENT = 'Student performance analytics for Canvas LMS data';

-- Grant access
GRANT USAGE ON SCHEMA ANALYTICS TO ROLE PUBLIC;
GRANT REFERENCES, SELECT ON SEMANTIC VIEW CANVAS_STUDENT_ANALYTICS TO ROLE PUBLIC;
GRANT SELECT ON VW_STUDENTS_BASE TO ROLE PUBLIC;


-- Semantic View 2: Course Analytics
CREATE OR REPLACE SEMANTIC VIEW CANVAS_COURSE_ANALYTICS

  TABLES (
    courses AS DEMO_CANVAS_DB.ANALYTICS.VW_COURSES_BASE
      PRIMARY KEY (course_id)
      COMMENT = 'Course dimension with curriculum info'
  )

  FACTS (
    courses.course_credits AS credit_hours
      COMMENT = 'Number of credit hours for the course',
    courses.max_seats AS max_enrollment
      COMMENT = 'Maximum enrollment capacity'
  )

  DIMENSIONS (
    courses.course_id AS course_id
      COMMENT = 'Unique course identifier',
    courses.course_code AS course_code
      COMMENT = 'Course code like CIS 4930',
    courses.course_name AS course_name
      COMMENT = 'Full course name',
    courses.department AS department
      COMMENT = 'Academic department',
    courses.term AS term
      COMMENT = 'Academic term',
    courses.instructor AS instructor_name
      COMMENT = 'Course instructor name',
    courses.delivery_mode AS delivery_mode
      COMMENT = 'Course delivery: In-Person, Online, Hybrid',
    courses.start_date AS start_date
      COMMENT = 'Course start date',
    courses.end_date AS end_date
      COMMENT = 'Course end date',
    courses.course_level AS course_level
      COMMENT = 'Course level: Undergraduate or Graduate'
  )

  METRICS (
    courses.total_courses AS COUNT(course_id)
      COMMENT = 'Total number of courses',
    courses.total_credit_hours AS SUM(credit_hours)
      COMMENT = 'Total credit hours offered',
    courses.total_capacity AS SUM(max_enrollment)
      COMMENT = 'Total enrollment capacity',
    courses.current_courses AS COUNT_IF(is_current = TRUE)
      COMMENT = 'Number of current courses',
    courses.unique_instructors AS COUNT(DISTINCT instructor_name)
      COMMENT = 'Number of unique instructors'
  )

  COMMENT = 'Course and curriculum analytics for Canvas LMS';

GRANT REFERENCES, SELECT ON SEMANTIC VIEW CANVAS_COURSE_ANALYTICS TO ROLE PUBLIC;
GRANT SELECT ON VW_COURSES_BASE TO ROLE PUBLIC;


-- Semantic View 3: Enrollment Analytics
CREATE OR REPLACE SEMANTIC VIEW CANVAS_ENROLLMENT_ANALYTICS

  TABLES (
    enrollments AS DEMO_CANVAS_DB.ANALYTICS.VW_ENROLLMENTS_BASE
      PRIMARY KEY (enrollment_id)
      COMMENT = 'Enrollment facts with student and course details'
  )

  FACTS (
    enrollments.grade_point AS grade_points
      COMMENT = 'Grade points earned 0.0 to 4.0',
    enrollments.credit_hours AS credit_hours
      COMMENT = 'Credit hours for the course'
  )

  DIMENSIONS (
    enrollments.enrollment_id AS enrollment_id
      COMMENT = 'Unique enrollment identifier',
    enrollments.student_id AS student_id
      COMMENT = 'Student identifier',
    enrollments.student_name AS student_name
      COMMENT = 'Full name of the student',
    enrollments.student_major AS student_major
      COMMENT = 'Major of the student',
    enrollments.student_classification AS student_classification
      COMMENT = 'Classification of the student',
    enrollments.course_id AS course_id
      COMMENT = 'Course identifier',
    enrollments.course_code AS course_code
      COMMENT = 'Course code',
    enrollments.course_name AS course_name
      COMMENT = 'Name of the course',
    enrollments.department AS department
      COMMENT = 'Department offering the course',
    enrollments.term AS term
      COMMENT = 'Academic term',
    enrollments.instructor AS instructor_name
      COMMENT = 'Course instructor',
    enrollments.enrollment_status AS enrollment_status
      COMMENT = 'Enrollment status',
    enrollments.enrollment_date AS enrollment_date
      COMMENT = 'Date of enrollment',
    enrollments.letter_grade AS final_grade
      COMMENT = 'Final letter grade',
    enrollments.grade_category AS grade_category
      COMMENT = 'Grade category based on points'
  )

  METRICS (
    enrollments.total_enrollments AS COUNT(enrollment_id)
      COMMENT = 'Total number of enrollments',
    enrollments.unique_students AS COUNT(DISTINCT student_id)
      COMMENT = 'Number of unique students',
    enrollments.unique_courses AS COUNT(DISTINCT course_id)
      COMMENT = 'Number of unique courses',
    enrollments.average_grade_points AS AVG(grade_points)
      COMMENT = 'Average grade points',
    enrollments.total_credit_hours AS SUM(credit_hours)
      COMMENT = 'Total credit hours enrolled'
  )

  COMMENT = 'Enrollment analytics combining student and course data';

GRANT REFERENCES, SELECT ON SEMANTIC VIEW CANVAS_ENROLLMENT_ANALYTICS TO ROLE PUBLIC;
GRANT SELECT ON VW_ENROLLMENTS_BASE TO ROLE PUBLIC;


-- Semantic View 4: Submission Analytics
CREATE OR REPLACE SEMANTIC VIEW CANVAS_SUBMISSION_ANALYTICS

  TABLES (
    submissions AS DEMO_CANVAS_DB.ANALYTICS.VW_SUBMISSIONS_BASE
      PRIMARY KEY (submission_id)
      COMMENT = 'Assignment submissions with grades'
  )

  FACTS (
    submissions.submission_score AS score
      COMMENT = 'Score received on the submission',
    submissions.max_points AS points_possible
      COMMENT = 'Maximum points possible',
    submissions.attempts AS attempt_number
      COMMENT = 'Number of submission attempts'
  )

  DIMENSIONS (
    submissions.submission_id AS submission_id
      COMMENT = 'Unique submission identifier',
    submissions.assignment_id AS assignment_id
      COMMENT = 'Assignment identifier',
    submissions.assignment_name AS assignment_name
      COMMENT = 'Name of the assignment',
    submissions.assignment_type AS assignment_type
      COMMENT = 'Type: Quiz, Homework, Exam, Project',
    submissions.student_id AS student_id
      COMMENT = 'Student identifier',
    submissions.student_name AS student_name
      COMMENT = 'Name of the student',
    submissions.student_major AS student_major
      COMMENT = 'Student major',
    submissions.submission_date AS submission_date
      COMMENT = 'Date of submission',
    submissions.due_date AS due_date
      COMMENT = 'Assignment due date',
    submissions.is_late AS late_flag
      COMMENT = 'Whether submission was late',
    submissions.grade AS grade
      COMMENT = 'Letter grade assigned',
    submissions.score_category AS score_category
      COMMENT = 'Score category based on percentage'
  )

  METRICS (
    submissions.total_submissions AS COUNT(submission_id)
      COMMENT = 'Total number of submissions',
    submissions.unique_students_submitted AS COUNT(DISTINCT student_id)
      COMMENT = 'Unique students who submitted',
    submissions.average_score AS AVG(score)
      COMMENT = 'Average score',
    submissions.late_submission_count AS COUNT_IF(late_flag = TRUE)
      COMMENT = 'Number of late submissions'
  )

  COMMENT = 'Assignment submission analytics for Canvas LMS';

GRANT REFERENCES, SELECT ON SEMANTIC VIEW CANVAS_SUBMISSION_ANALYTICS TO ROLE PUBLIC;
GRANT SELECT ON VW_SUBMISSIONS_BASE TO ROLE PUBLIC;


-- Semantic View 5: Performance Dashboard
CREATE OR REPLACE SEMANTIC VIEW CANVAS_PERFORMANCE_DASHBOARD

  TABLES (
    performance AS DEMO_CANVAS_DB.ANALYTICS.VW_PERFORMANCE_BASE
      COMMENT = 'Aggregated student-course performance'
  )

  FACTS (
    performance.assignments_total AS total_assignments
      COMMENT = 'Total assignments in the course',
    performance.assignments_completed AS completed_assignments
      COMMENT = 'Assignments completed by student',
    performance.avg_assignment_score AS average_score
      COMMENT = 'Average score on assignments',
    performance.points_earned AS total_points_earned
      COMMENT = 'Total points earned',
    performance.points_possible AS total_points_possible
      COMMENT = 'Total points possible',
    performance.late_count AS late_submissions
      COMMENT = 'Number of late submissions',
    performance.missing_count AS missing_submissions
      COMMENT = 'Number of missing submissions'
  )

  DIMENSIONS (
    performance.student_id AS student_id
      COMMENT = 'Student identifier',
    performance.student_name AS student_name
      COMMENT = 'Student full name',
    performance.major AS major
      COMMENT = 'Student major',
    performance.classification AS classification
      COMMENT = 'Student classification',
    performance.overall_gpa AS overall_gpa
      COMMENT = 'Student overall GPA',
    performance.course_id AS course_id
      COMMENT = 'Course identifier',
    performance.course_code AS course_code
      COMMENT = 'Course code',
    performance.course_name AS course_name
      COMMENT = 'Course name',
    performance.department AS department
      COMMENT = 'Academic department',
    performance.instructor AS instructor_name
      COMMENT = 'Course instructor',
    performance.term AS term
      COMMENT = 'Academic term',
    performance.course_grade AS performance_grade
      COMMENT = 'Current grade in course',
    performance.completion_rate AS completion_rate
      COMMENT = 'Assignment completion percentage'
  )

  METRICS (
    performance.total_records AS COUNT(*)
      COMMENT = 'Total student-course combinations',
    performance.avg_course_score AS AVG(average_score)
      COMMENT = 'Average score across all',
    performance.total_late_submissions AS SUM(late_submissions)
      COMMENT = 'Total late submissions',
    performance.students_at_risk AS COUNT_IF(average_score < 60)
      COMMENT = 'Students scoring below 60 percent',
    performance.high_performers AS COUNT_IF(average_score >= 90)
      COMMENT = 'Students scoring 90 percent or above'
  )

  COMMENT = 'Aggregated student performance metrics for dashboards';

GRANT REFERENCES, SELECT ON SEMANTIC VIEW CANVAS_PERFORMANCE_DASHBOARD TO ROLE PUBLIC;
GRANT SELECT ON VW_PERFORMANCE_BASE TO ROLE PUBLIC;


-- ============================================================================
-- VERIFICATION
-- ============================================================================

-- Show all created views
SHOW VIEWS IN SCHEMA DEMO_CANVAS_DB.ANALYTICS;

-- Show all created semantic views
SHOW SEMANTIC VIEWS IN SCHEMA DEMO_CANVAS_DB.ANALYTICS;

-- Describe a semantic view
DESCRIBE SEMANTIC VIEW CANVAS_STUDENT_ANALYTICS;

SELECT 'Semantic views setup complete!' AS status;
