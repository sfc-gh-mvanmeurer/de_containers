/*
================================================================================
FGCU Canvas Analytics - Semantic Views Setup
================================================================================
Creates semantic views on top of the curated Canvas LMS data to enable 
natural language queries through Cortex Analyst and Snowflake Intelligence.

This script:
1. Creates base views with computed columns
2. Creates semantic views referencing those base views

Reference: https://docs.snowflake.com/en/user-guide/views-semantic/overview
================================================================================
*/

-- ============================================================================
-- SETUP
-- ============================================================================
USE ROLE ACCOUNTADMIN;
USE DATABASE FGCU_CANVAS_DEMO;
USE WAREHOUSE FGCU_TRANSFORM_WH;

-- Create analytics schema for semantic views
CREATE SCHEMA IF NOT EXISTS ANALYTICS
    COMMENT = 'Schema for semantic views and analytics objects';

USE SCHEMA ANALYTICS;

-- ============================================================================
-- STEP 1: CREATE BASE VIEWS
-- These views add computed columns that semantic views will reference
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
FROM FGCU_CANVAS_DEMO.CURATED.DIM_STUDENTS;

-- Base view for courses with computed columns
CREATE OR REPLACE VIEW VW_COURSES_BASE AS
SELECT 
    course_id,
    canvas_course_id,
    course_code,
    course_name,
    department,
    credits,
    term,
    instructor_name,
    max_enrollment,
    start_date,
    end_date,
    status,
    CASE 
        WHEN course_code LIKE '%1___' THEN 'Freshman'
        WHEN course_code LIKE '%2___' THEN 'Sophomore'
        WHEN course_code LIKE '%3___' THEN 'Junior'
        WHEN course_code LIKE '%4___' THEN 'Senior'
        ELSE 'Upper Level'
    END AS course_level,
    created_at
FROM FGCU_CANVAS_DEMO.CURATED.DIM_COURSES;

-- Base view for enrollments with computed columns
CREATE OR REPLACE VIEW VW_ENROLLMENTS_BASE AS
SELECT 
    e.enrollment_id,
    e.student_id,
    e.course_id,
    e.enrollment_date,
    e.enrollment_status,
    e.final_grade,
    e.grade_points,
    e.last_activity_date,
    CASE 
        WHEN e.grade_points >= 3.5 THEN 'Excellent'
        WHEN e.grade_points >= 2.5 THEN 'Good'
        WHEN e.grade_points >= 1.5 THEN 'Satisfactory'
        WHEN e.grade_points >= 0.5 THEN 'Poor'
        WHEN e.grade_points IS NOT NULL THEN 'Failing'
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
    c.credits,
    e.created_at
FROM FGCU_CANVAS_DEMO.CURATED.FACT_ENROLLMENTS e
LEFT JOIN FGCU_CANVAS_DEMO.CURATED.DIM_STUDENTS s ON e.student_id = s.student_id
LEFT JOIN FGCU_CANVAS_DEMO.CURATED.DIM_COURSES c ON e.course_id = c.course_id;

-- Base view for submissions with computed columns
CREATE OR REPLACE VIEW VW_SUBMISSIONS_BASE AS
SELECT 
    sub.submission_id,
    sub.assignment_id,
    sub.student_id,
    sub.submission_date,
    sub.score,
    sub.grade,
    sub.late_flag,
    sub.attempt_number,
    sub.graded_at,
    sub.grader_id,
    a.assignment_name,
    a.assignment_type,
    a.points_possible,
    a.due_date,
    a.course_id,
    CASE 
        WHEN a.points_possible > 0 AND sub.score >= a.points_possible * 0.9 THEN 'Excellent'
        WHEN a.points_possible > 0 AND sub.score >= a.points_possible * 0.8 THEN 'Good'
        WHEN a.points_possible > 0 AND sub.score >= a.points_possible * 0.7 THEN 'Satisfactory'
        WHEN a.points_possible > 0 AND sub.score >= a.points_possible * 0.6 THEN 'Below Average'
        ELSE 'Failing'
    END AS score_category,
    s.first_name AS student_first_name,
    s.last_name AS student_last_name,
    CONCAT(s.first_name, ' ', s.last_name) AS student_name,
    s.major AS student_major,
    sub.created_at
FROM FGCU_CANVAS_DEMO.CURATED.FACT_SUBMISSIONS sub
LEFT JOIN FGCU_CANVAS_DEMO.CURATED.DIM_ASSIGNMENTS a ON sub.assignment_id = a.assignment_id
LEFT JOIN FGCU_CANVAS_DEMO.CURATED.DIM_STUDENTS s ON sub.student_id = s.student_id;

-- Base view for performance aggregates
CREATE OR REPLACE VIEW VW_PERFORMANCE_BASE AS
SELECT 
    p.student_id,
    p.course_id,
    p.term,
    p.total_assignments,
    p.completed_assignments,
    p.average_score,
    p.total_points_earned,
    p.total_points_possible,
    p.late_submissions,
    p.on_time_submissions,
    p.performance_grade,
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
FROM FGCU_CANVAS_DEMO.CURATED.AGG_STUDENT_COURSE_PERFORMANCE p
LEFT JOIN FGCU_CANVAS_DEMO.CURATED.DIM_STUDENTS s ON p.student_id = s.student_id
LEFT JOIN FGCU_CANVAS_DEMO.CURATED.DIM_COURSES c ON p.course_id = c.course_id;


-- ============================================================================
-- STEP 2: CREATE SEMANTIC VIEWS
-- Reference the base views created above
-- ============================================================================

-- Semantic View 1: Student Analytics
CREATE OR REPLACE SEMANTIC VIEW CANVAS_STUDENT_ANALYTICS
  TABLES (
    STUDENTS AS FGCU_CANVAS_DEMO.ANALYTICS.VW_STUDENTS_BASE
      PRIMARY KEY (student_id)
  )
  FACTS (
    student_gpa AS STUDENTS.gpa
      DESCRIPTION 'Individual student GPA on 4.0 scale'
  )
  DIMENSIONS (
    student_id AS STUDENTS.student_id
      DESCRIPTION 'Unique student identifier',
    student_name AS STUDENTS.full_name
      DESCRIPTION 'Full name of the student',
    first_name AS STUDENTS.first_name
      DESCRIPTION 'Student first name',
    last_name AS STUDENTS.last_name
      DESCRIPTION 'Student last name',
    email AS STUDENTS.email
      DESCRIPTION 'Student email address',
    major AS STUDENTS.major
      DESCRIPTION 'Student academic major',
    classification AS STUDENTS.classification
      DESCRIPTION 'Student year: Freshman, Sophomore, Junior, Senior',
    enrollment_status AS STUDENTS.enrollment_status
      DESCRIPTION 'Current enrollment status: Active, Inactive, Graduated',
    enrollment_date AS STUDENTS.enrollment_date
      DESCRIPTION 'Date student first enrolled',
    graduation_date AS STUDENTS.expected_graduation
      DESCRIPTION 'Expected graduation date',
    academic_standing AS STUDENTS.academic_standing
      DESCRIPTION 'Academic standing based on GPA'
  )
  METRICS (
    total_students AS COUNT(STUDENTS.student_id)
      DESCRIPTION 'Total number of students',
    average_gpa AS AVG(STUDENTS.gpa)
      DESCRIPTION 'Average GPA across students',
    active_students AS COUNT_IF(STUDENTS.enrollment_status = 'Active')
      DESCRIPTION 'Number of currently active students',
    at_risk_students AS COUNT_IF(STUDENTS.gpa < 2.0)
      DESCRIPTION 'Students with GPA below 2.0',
    deans_list_students AS COUNT_IF(STUDENTS.gpa >= 3.5)
      DESCRIPTION 'Students on Deans List with GPA >= 3.5'
  )
  COMMENT = 'Student performance analytics for Canvas LMS data';

GRANT USAGE ON SCHEMA ANALYTICS TO ROLE PUBLIC;
GRANT REFERENCES ON SEMANTIC VIEW CANVAS_STUDENT_ANALYTICS TO ROLE PUBLIC;
GRANT SELECT ON VW_STUDENTS_BASE TO ROLE PUBLIC;


-- Semantic View 2: Course Analytics
CREATE OR REPLACE SEMANTIC VIEW CANVAS_COURSE_ANALYTICS
  TABLES (
    COURSES AS FGCU_CANVAS_DEMO.ANALYTICS.VW_COURSES_BASE
      PRIMARY KEY (course_id)
  )
  FACTS (
    course_credits AS COURSES.credits
      DESCRIPTION 'Number of credit hours for the course',
    max_seats AS COURSES.max_enrollment
      DESCRIPTION 'Maximum enrollment capacity'
  )
  DIMENSIONS (
    course_id AS COURSES.course_id
      DESCRIPTION 'Unique course identifier',
    course_code AS COURSES.course_code
      DESCRIPTION 'Course code like CIS 4930',
    course_name AS COURSES.course_name
      DESCRIPTION 'Full course name',
    department AS COURSES.department
      DESCRIPTION 'Academic department',
    term AS COURSES.term
      DESCRIPTION 'Academic term',
    instructor AS COURSES.instructor_name
      DESCRIPTION 'Course instructor name',
    course_status AS COURSES.status
      DESCRIPTION 'Course status: Active, Completed',
    start_date AS COURSES.start_date
      DESCRIPTION 'Course start date',
    end_date AS COURSES.end_date
      DESCRIPTION 'Course end date',
    course_level AS COURSES.course_level
      DESCRIPTION 'Course level based on number'
  )
  METRICS (
    total_courses AS COUNT(COURSES.course_id)
      DESCRIPTION 'Total number of courses',
    total_credit_hours AS SUM(COURSES.credits)
      DESCRIPTION 'Total credit hours offered',
    total_capacity AS SUM(COURSES.max_enrollment)
      DESCRIPTION 'Total enrollment capacity',
    active_courses AS COUNT_IF(COURSES.status = 'Active')
      DESCRIPTION 'Number of active courses',
    unique_instructors AS COUNT(DISTINCT COURSES.instructor_name)
      DESCRIPTION 'Number of unique instructors'
  )
  COMMENT = 'Course and curriculum analytics for Canvas LMS';

GRANT REFERENCES ON SEMANTIC VIEW CANVAS_COURSE_ANALYTICS TO ROLE PUBLIC;
GRANT SELECT ON VW_COURSES_BASE TO ROLE PUBLIC;


-- Semantic View 3: Enrollment Analytics
CREATE OR REPLACE SEMANTIC VIEW CANVAS_ENROLLMENT_ANALYTICS
  TABLES (
    ENROLLMENTS AS FGCU_CANVAS_DEMO.ANALYTICS.VW_ENROLLMENTS_BASE
      PRIMARY KEY (enrollment_id)
  )
  FACTS (
    grade_point AS ENROLLMENTS.grade_points
      DESCRIPTION 'Grade points earned 0.0 to 4.0',
    credit_hours AS ENROLLMENTS.credits
      DESCRIPTION 'Credit hours for the course'
  )
  DIMENSIONS (
    enrollment_id AS ENROLLMENTS.enrollment_id
      DESCRIPTION 'Unique enrollment identifier',
    student_id AS ENROLLMENTS.student_id
      DESCRIPTION 'Student identifier',
    student_name AS ENROLLMENTS.student_name
      DESCRIPTION 'Full name of the student',
    student_major AS ENROLLMENTS.student_major
      DESCRIPTION 'Major of the student',
    student_classification AS ENROLLMENTS.student_classification
      DESCRIPTION 'Classification of the student',
    course_id AS ENROLLMENTS.course_id
      DESCRIPTION 'Course identifier',
    course_code AS ENROLLMENTS.course_code
      DESCRIPTION 'Course code',
    course_name AS ENROLLMENTS.course_name
      DESCRIPTION 'Name of the course',
    department AS ENROLLMENTS.department
      DESCRIPTION 'Department offering the course',
    term AS ENROLLMENTS.term
      DESCRIPTION 'Academic term',
    instructor AS ENROLLMENTS.instructor_name
      DESCRIPTION 'Course instructor',
    enrollment_status AS ENROLLMENTS.enrollment_status
      DESCRIPTION 'Enrollment status',
    enrollment_date AS ENROLLMENTS.enrollment_date
      DESCRIPTION 'Date of enrollment',
    letter_grade AS ENROLLMENTS.final_grade
      DESCRIPTION 'Final letter grade',
    grade_category AS ENROLLMENTS.grade_category
      DESCRIPTION 'Grade category based on points'
  )
  METRICS (
    total_enrollments AS COUNT(ENROLLMENTS.enrollment_id)
      DESCRIPTION 'Total number of enrollments',
    unique_students AS COUNT(DISTINCT ENROLLMENTS.student_id)
      DESCRIPTION 'Number of unique students',
    unique_courses AS COUNT(DISTINCT ENROLLMENTS.course_id)
      DESCRIPTION 'Number of unique courses',
    average_grade_points AS AVG(ENROLLMENTS.grade_points)
      DESCRIPTION 'Average grade points',
    total_credit_hours AS SUM(ENROLLMENTS.credits)
      DESCRIPTION 'Total credit hours enrolled',
    completion_rate AS COUNT_IF(ENROLLMENTS.enrollment_status = 'Completed') * 100.0 / NULLIF(COUNT(ENROLLMENTS.enrollment_id), 0)
      DESCRIPTION 'Percentage of enrollments completed',
    pass_rate AS COUNT_IF(ENROLLMENTS.grade_points >= 2.0) * 100.0 / NULLIF(COUNT_IF(ENROLLMENTS.grade_points IS NOT NULL), 0)
      DESCRIPTION 'Percentage passing with C or better'
  )
  COMMENT = 'Enrollment analytics combining student and course data';

GRANT REFERENCES ON SEMANTIC VIEW CANVAS_ENROLLMENT_ANALYTICS TO ROLE PUBLIC;
GRANT SELECT ON VW_ENROLLMENTS_BASE TO ROLE PUBLIC;


-- Semantic View 4: Submission Analytics
CREATE OR REPLACE SEMANTIC VIEW CANVAS_SUBMISSION_ANALYTICS
  TABLES (
    SUBMISSIONS AS FGCU_CANVAS_DEMO.ANALYTICS.VW_SUBMISSIONS_BASE
      PRIMARY KEY (submission_id)
  )
  FACTS (
    submission_score AS SUBMISSIONS.score
      DESCRIPTION 'Score received on the submission',
    max_points AS SUBMISSIONS.points_possible
      DESCRIPTION 'Maximum points possible',
    attempts AS SUBMISSIONS.attempt_number
      DESCRIPTION 'Number of submission attempts'
  )
  DIMENSIONS (
    submission_id AS SUBMISSIONS.submission_id
      DESCRIPTION 'Unique submission identifier',
    assignment_id AS SUBMISSIONS.assignment_id
      DESCRIPTION 'Assignment identifier',
    assignment_name AS SUBMISSIONS.assignment_name
      DESCRIPTION 'Name of the assignment',
    assignment_type AS SUBMISSIONS.assignment_type
      DESCRIPTION 'Type: Quiz, Homework, Exam, Project',
    student_id AS SUBMISSIONS.student_id
      DESCRIPTION 'Student identifier',
    student_name AS SUBMISSIONS.student_name
      DESCRIPTION 'Name of the student',
    student_major AS SUBMISSIONS.student_major
      DESCRIPTION 'Student major',
    submission_date AS SUBMISSIONS.submission_date
      DESCRIPTION 'Date of submission',
    due_date AS SUBMISSIONS.due_date
      DESCRIPTION 'Assignment due date',
    is_late AS SUBMISSIONS.late_flag
      DESCRIPTION 'Whether submission was late',
    grade AS SUBMISSIONS.grade
      DESCRIPTION 'Letter grade assigned',
    score_category AS SUBMISSIONS.score_category
      DESCRIPTION 'Score category based on percentage'
  )
  METRICS (
    total_submissions AS COUNT(SUBMISSIONS.submission_id)
      DESCRIPTION 'Total number of submissions',
    unique_students_submitted AS COUNT(DISTINCT SUBMISSIONS.student_id)
      DESCRIPTION 'Unique students who submitted',
    average_score AS AVG(SUBMISSIONS.score)
      DESCRIPTION 'Average score',
    late_submission_count AS COUNT_IF(SUBMISSIONS.late_flag = TRUE)
      DESCRIPTION 'Number of late submissions',
    late_submission_rate AS COUNT_IF(SUBMISSIONS.late_flag = TRUE) * 100.0 / NULLIF(COUNT(SUBMISSIONS.submission_id), 0)
      DESCRIPTION 'Percentage of late submissions'
  )
  COMMENT = 'Assignment submission analytics for Canvas LMS';

GRANT REFERENCES ON SEMANTIC VIEW CANVAS_SUBMISSION_ANALYTICS TO ROLE PUBLIC;
GRANT SELECT ON VW_SUBMISSIONS_BASE TO ROLE PUBLIC;


-- Semantic View 5: Performance Dashboard
CREATE OR REPLACE SEMANTIC VIEW CANVAS_PERFORMANCE_DASHBOARD
  TABLES (
    PERFORMANCE AS FGCU_CANVAS_DEMO.ANALYTICS.VW_PERFORMANCE_BASE
  )
  FACTS (
    assignments_total AS PERFORMANCE.total_assignments
      DESCRIPTION 'Total assignments in the course',
    assignments_completed AS PERFORMANCE.completed_assignments
      DESCRIPTION 'Assignments completed by student',
    avg_assignment_score AS PERFORMANCE.average_score
      DESCRIPTION 'Average score on assignments',
    points_earned AS PERFORMANCE.total_points_earned
      DESCRIPTION 'Total points earned',
    points_possible AS PERFORMANCE.total_points_possible
      DESCRIPTION 'Total points possible',
    late_count AS PERFORMANCE.late_submissions
      DESCRIPTION 'Number of late submissions',
    ontime_count AS PERFORMANCE.on_time_submissions
      DESCRIPTION 'Number of on-time submissions'
  )
  DIMENSIONS (
    student_id AS PERFORMANCE.student_id
      DESCRIPTION 'Student identifier',
    student_name AS PERFORMANCE.student_name
      DESCRIPTION 'Student full name',
    major AS PERFORMANCE.major
      DESCRIPTION 'Student major',
    classification AS PERFORMANCE.classification
      DESCRIPTION 'Student classification',
    overall_gpa AS PERFORMANCE.overall_gpa
      DESCRIPTION 'Student overall GPA',
    course_id AS PERFORMANCE.course_id
      DESCRIPTION 'Course identifier',
    course_code AS PERFORMANCE.course_code
      DESCRIPTION 'Course code',
    course_name AS PERFORMANCE.course_name
      DESCRIPTION 'Course name',
    department AS PERFORMANCE.department
      DESCRIPTION 'Academic department',
    instructor AS PERFORMANCE.instructor_name
      DESCRIPTION 'Course instructor',
    term AS PERFORMANCE.term
      DESCRIPTION 'Academic term',
    course_grade AS PERFORMANCE.performance_grade
      DESCRIPTION 'Current grade in course',
    completion_rate AS PERFORMANCE.completion_rate
      DESCRIPTION 'Assignment completion percentage'
  )
  METRICS (
    total_records AS COUNT(*)
      DESCRIPTION 'Total student-course combinations',
    avg_course_score AS AVG(PERFORMANCE.average_score)
      DESCRIPTION 'Average score across all',
    total_late_submissions AS SUM(PERFORMANCE.late_submissions)
      DESCRIPTION 'Total late submissions',
    students_at_risk AS COUNT_IF(PERFORMANCE.average_score < 60)
      DESCRIPTION 'Students scoring below 60 percent',
    high_performers AS COUNT_IF(PERFORMANCE.average_score >= 90)
      DESCRIPTION 'Students scoring 90 percent or above'
  )
  COMMENT = 'Aggregated student performance metrics for dashboards';

GRANT REFERENCES ON SEMANTIC VIEW CANVAS_PERFORMANCE_DASHBOARD TO ROLE PUBLIC;
GRANT SELECT ON VW_PERFORMANCE_BASE TO ROLE PUBLIC;


-- ============================================================================
-- VERIFICATION
-- ============================================================================

-- Show all created views
SHOW VIEWS IN SCHEMA FGCU_CANVAS_DEMO.ANALYTICS;

-- Show all created semantic views
SHOW SEMANTIC VIEWS IN SCHEMA FGCU_CANVAS_DEMO.ANALYTICS;

-- Test a semantic view
DESCRIBE SEMANTIC VIEW CANVAS_STUDENT_ANALYTICS;

SELECT 'Setup complete!' AS status;
