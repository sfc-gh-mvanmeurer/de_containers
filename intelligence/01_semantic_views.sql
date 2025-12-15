/*
================================================================================
FGCU Canvas Analytics - Semantic Views Setup
================================================================================
Creates semantic views on top of the curated Canvas LMS data to enable 
natural language queries through Cortex Analyst and Snowflake Intelligence.

Semantic views define:
- DIMENSIONS: Categorical attributes (who, what, where, when)
- METRICS: Aggregated measures (KPIs, calculations)
- FACTS: Row-level numeric attributes
- RELATIONSHIPS: How tables join together

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
-- SEMANTIC VIEW 1: STUDENT ANALYTICS
-- Business-focused view of student data for performance analysis
-- ============================================================================

CREATE OR REPLACE SEMANTIC VIEW CANVAS_STUDENT_ANALYTICS
  COMMENT = 'Student performance analytics for Canvas LMS data. Use this view to analyze student grades, enrollment patterns, and academic performance by major, classification, and other student attributes.'
  AS
    -- Define the logical tables
    TABLES (
        -- Student dimension table
        STUDENTS AS (
            SELECT 
                student_id,
                first_name,
                last_name,
                first_name || ' ' || last_name AS full_name,
                email,
                major,
                classification,
                enrollment_status,
                enrollment_date,
                expected_graduation,
                gpa,
                advisor_id,
                created_at,
                updated_at
            FROM FGCU_CANVAS_DEMO.CURATED.DIM_STUDENTS
        )
        
        -- Facts: Row-level numeric attributes
        FACTS (
            student_gpa AS students.gpa
                DESCRIPTION 'Individual student GPA on 4.0 scale'
        )
        
        -- Dimensions: Categorical attributes for grouping/filtering
        DIMENSIONS (
            student_id AS students.student_id
                DESCRIPTION 'Unique student identifier (e.g., U12345678)'
                PRIMARY KEY,
            
            student_name AS students.full_name
                DESCRIPTION 'Full name of the student',
            
            first_name AS students.first_name
                DESCRIPTION 'Student first name',
            
            last_name AS students.last_name
                DESCRIPTION 'Student last name',
            
            email AS students.email
                DESCRIPTION 'Student email address',
            
            major AS students.major
                DESCRIPTION 'Student academic major (e.g., Computer Science, Biology, Business)',
            
            classification AS students.classification
                DESCRIPTION 'Student year classification: Freshman, Sophomore, Junior, Senior, Graduate',
            
            enrollment_status AS students.enrollment_status
                DESCRIPTION 'Current enrollment status: Active, Inactive, Graduated, Withdrawn',
            
            enrollment_date AS students.enrollment_date
                DESCRIPTION 'Date student first enrolled',
            
            graduation_date AS students.expected_graduation
                DESCRIPTION 'Expected graduation date',
            
            academic_standing AS 
                CASE 
                    WHEN students.gpa >= 3.5 THEN 'Dean''s List'
                    WHEN students.gpa >= 3.0 THEN 'Good Standing'
                    WHEN students.gpa >= 2.0 THEN 'Satisfactory'
                    ELSE 'Academic Probation'
                END
                DESCRIPTION 'Academic standing based on GPA thresholds'
        )
        
        -- Metrics: Aggregated measures
        METRICS (
            total_students AS COUNT(students.student_id)
                DESCRIPTION 'Total number of students',
            
            average_gpa AS AVG(students.gpa)
                DESCRIPTION 'Average GPA across students',
            
            median_gpa AS MEDIAN(students.gpa)
                DESCRIPTION 'Median GPA across students',
            
            min_gpa AS MIN(students.gpa)
                DESCRIPTION 'Minimum GPA',
            
            max_gpa AS MAX(students.gpa)
                DESCRIPTION 'Maximum GPA',
            
            active_students AS COUNT_IF(students.enrollment_status = 'Active')
                DESCRIPTION 'Number of currently active students',
            
            at_risk_students AS COUNT_IF(students.gpa < 2.0)
                DESCRIPTION 'Number of students with GPA below 2.0 (academic probation)',
            
            deans_list_students AS COUNT_IF(students.gpa >= 3.5)
                DESCRIPTION 'Number of students on Dean''s List (GPA >= 3.5)',
            
            graduation_rate AS 
                COUNT_IF(students.enrollment_status = 'Graduated') * 100.0 / NULLIF(COUNT(students.student_id), 0)
                DESCRIPTION 'Percentage of students who have graduated'
        )
    );

-- Grant access to the semantic view
GRANT USAGE ON SCHEMA ANALYTICS TO ROLE PUBLIC;
GRANT REFERENCES ON SEMANTIC VIEW CANVAS_STUDENT_ANALYTICS TO ROLE PUBLIC;
GRANT SELECT ON FGCU_CANVAS_DEMO.CURATED.DIM_STUDENTS TO ROLE PUBLIC;


-- ============================================================================
-- SEMANTIC VIEW 2: COURSE ANALYTICS
-- Business-focused view of course data for curriculum analysis
-- ============================================================================

CREATE OR REPLACE SEMANTIC VIEW CANVAS_COURSE_ANALYTICS
  COMMENT = 'Course and curriculum analytics for Canvas LMS. Use this view to analyze course enrollments, department performance, and instructor effectiveness.'
  AS
    TABLES (
        -- Course dimension table
        COURSES AS (
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
                created_at
            FROM FGCU_CANVAS_DEMO.CURATED.DIM_COURSES
        )
        
        FACTS (
            course_credits AS courses.credits
                DESCRIPTION 'Number of credit hours for the course',
            
            max_seats AS courses.max_enrollment
                DESCRIPTION 'Maximum enrollment capacity'
        )
        
        DIMENSIONS (
            course_id AS courses.course_id
                DESCRIPTION 'Unique course identifier'
                PRIMARY KEY,
            
            course_code AS courses.course_code
                DESCRIPTION 'Course code (e.g., CIS 4930, BIO 101)',
            
            course_name AS courses.course_name
                DESCRIPTION 'Full course name',
            
            department AS courses.department
                DESCRIPTION 'Academic department offering the course',
            
            term AS courses.term
                DESCRIPTION 'Academic term (e.g., Fall 2024, Spring 2025)',
            
            instructor AS courses.instructor_name
                DESCRIPTION 'Name of the course instructor',
            
            course_status AS courses.status
                DESCRIPTION 'Course status: Active, Completed, Cancelled',
            
            start_date AS courses.start_date
                DESCRIPTION 'Course start date',
            
            end_date AS courses.end_date
                DESCRIPTION 'Course end date',
            
            course_level AS 
                CASE 
                    WHEN courses.course_code LIKE '%1___' THEN 'Freshman'
                    WHEN courses.course_code LIKE '%2___' THEN 'Sophomore'
                    WHEN courses.course_code LIKE '%3___' THEN 'Junior'
                    WHEN courses.course_code LIKE '%4___' THEN 'Senior'
                    WHEN courses.course_code LIKE '%5___' OR courses.course_code LIKE '%6___' THEN 'Graduate'
                    ELSE 'Other'
                END
                DESCRIPTION 'Course level based on course number'
        )
        
        METRICS (
            total_courses AS COUNT(courses.course_id)
                DESCRIPTION 'Total number of courses',
            
            total_credit_hours AS SUM(courses.credits)
                DESCRIPTION 'Total credit hours offered',
            
            avg_credits AS AVG(courses.credits)
                DESCRIPTION 'Average credit hours per course',
            
            total_capacity AS SUM(courses.max_enrollment)
                DESCRIPTION 'Total enrollment capacity across all courses',
            
            active_courses AS COUNT_IF(courses.status = 'Active')
                DESCRIPTION 'Number of currently active courses',
            
            unique_instructors AS COUNT(DISTINCT courses.instructor_name)
                DESCRIPTION 'Number of unique instructors',
            
            unique_departments AS COUNT(DISTINCT courses.department)
                DESCRIPTION 'Number of academic departments'
        )
    );

GRANT REFERENCES ON SEMANTIC VIEW CANVAS_COURSE_ANALYTICS TO ROLE PUBLIC;
GRANT SELECT ON FGCU_CANVAS_DEMO.CURATED.DIM_COURSES TO ROLE PUBLIC;


-- ============================================================================
-- SEMANTIC VIEW 3: ENROLLMENT ANALYTICS
-- Business-focused view combining students, courses, and enrollments
-- ============================================================================

CREATE OR REPLACE SEMANTIC VIEW CANVAS_ENROLLMENT_ANALYTICS
  COMMENT = 'Enrollment analytics combining student and course data. Use this view to analyze enrollment patterns, course popularity, and student course loads.'
  AS
    TABLES (
        -- Enrollment fact table
        ENROLLMENTS AS (
            SELECT 
                enrollment_id,
                student_id,
                course_id,
                enrollment_date,
                enrollment_status,
                final_grade,
                grade_points,
                last_activity_date,
                created_at
            FROM FGCU_CANVAS_DEMO.CURATED.FACT_ENROLLMENTS
        ),
        
        -- Student dimension for joins
        STUDENTS AS (
            SELECT 
                student_id,
                first_name || ' ' || last_name AS student_name,
                major,
                classification,
                enrollment_status AS student_status,
                gpa
            FROM FGCU_CANVAS_DEMO.CURATED.DIM_STUDENTS
        ),
        
        -- Course dimension for joins
        COURSES AS (
            SELECT 
                course_id,
                course_code,
                course_name,
                department,
                term,
                instructor_name,
                credits
            FROM FGCU_CANVAS_DEMO.CURATED.DIM_COURSES
        )
        
        -- Define relationships between tables
        RELATIONSHIPS (
            enrollments (student_id) REFERENCES students (student_id),
            enrollments (course_id) REFERENCES courses (course_id)
        )
        
        FACTS (
            grade AS enrollments.final_grade
                DESCRIPTION 'Final letter grade for the enrollment',
            
            grade_point AS enrollments.grade_points
                DESCRIPTION 'Grade points earned (0.0 - 4.0 scale)',
            
            credit_hours AS courses.credits
                DESCRIPTION 'Credit hours for the course'
        )
        
        DIMENSIONS (
            enrollment_id AS enrollments.enrollment_id
                DESCRIPTION 'Unique enrollment identifier'
                PRIMARY KEY,
            
            student_id AS enrollments.student_id
                DESCRIPTION 'Student identifier',
            
            student_name AS students.student_name
                DESCRIPTION 'Full name of the enrolled student',
            
            student_major AS students.major
                DESCRIPTION 'Major of the enrolled student',
            
            student_classification AS students.classification
                DESCRIPTION 'Classification of the enrolled student',
            
            course_id AS enrollments.course_id
                DESCRIPTION 'Course identifier',
            
            course_code AS courses.course_code
                DESCRIPTION 'Course code',
            
            course_name AS courses.course_name
                DESCRIPTION 'Name of the course',
            
            department AS courses.department
                DESCRIPTION 'Department offering the course',
            
            term AS courses.term
                DESCRIPTION 'Academic term of enrollment',
            
            instructor AS courses.instructor_name
                DESCRIPTION 'Course instructor',
            
            enrollment_status AS enrollments.enrollment_status
                DESCRIPTION 'Enrollment status: Active, Completed, Dropped, Withdrawn',
            
            enrollment_date AS enrollments.enrollment_date
                DESCRIPTION 'Date of enrollment',
            
            letter_grade AS enrollments.final_grade
                DESCRIPTION 'Final letter grade (A, B, C, D, F)',
            
            grade_category AS 
                CASE 
                    WHEN enrollments.grade_points >= 3.5 THEN 'Excellent (A/A-)'
                    WHEN enrollments.grade_points >= 2.5 THEN 'Good (B/B+/B-)'
                    WHEN enrollments.grade_points >= 1.5 THEN 'Satisfactory (C/C+/C-)'
                    WHEN enrollments.grade_points >= 0.5 THEN 'Poor (D/D+/D-)'
                    WHEN enrollments.grade_points IS NOT NULL THEN 'Failing (F)'
                    ELSE 'Not Graded'
                END
                DESCRIPTION 'Grade category based on grade points'
        )
        
        METRICS (
            total_enrollments AS COUNT(enrollments.enrollment_id)
                DESCRIPTION 'Total number of enrollments',
            
            unique_students AS COUNT(DISTINCT enrollments.student_id)
                DESCRIPTION 'Number of unique students enrolled',
            
            unique_courses AS COUNT(DISTINCT enrollments.course_id)
                DESCRIPTION 'Number of unique courses with enrollments',
            
            average_grade_points AS AVG(enrollments.grade_points)
                DESCRIPTION 'Average grade points across enrollments',
            
            total_credit_hours AS SUM(courses.credits)
                DESCRIPTION 'Total credit hours enrolled',
            
            avg_credits_per_student AS 
                SUM(courses.credits) / NULLIF(COUNT(DISTINCT enrollments.student_id), 0)
                DESCRIPTION 'Average credit hours per student',
            
            completion_rate AS 
                COUNT_IF(enrollments.enrollment_status = 'Completed') * 100.0 / NULLIF(COUNT(enrollments.enrollment_id), 0)
                DESCRIPTION 'Percentage of enrollments completed',
            
            dropout_rate AS 
                COUNT_IF(enrollments.enrollment_status IN ('Dropped', 'Withdrawn')) * 100.0 / NULLIF(COUNT(enrollments.enrollment_id), 0)
                DESCRIPTION 'Percentage of enrollments dropped or withdrawn',
            
            pass_rate AS 
                COUNT_IF(enrollments.grade_points >= 2.0) * 100.0 / NULLIF(COUNT_IF(enrollments.grade_points IS NOT NULL), 0)
                DESCRIPTION 'Percentage of graded enrollments with passing grade (C or better)',
            
            a_grade_count AS COUNT_IF(enrollments.final_grade LIKE 'A%')
                DESCRIPTION 'Number of A grades',
            
            b_grade_count AS COUNT_IF(enrollments.final_grade LIKE 'B%')
                DESCRIPTION 'Number of B grades',
            
            c_grade_count AS COUNT_IF(enrollments.final_grade LIKE 'C%')
                DESCRIPTION 'Number of C grades',
            
            d_grade_count AS COUNT_IF(enrollments.final_grade LIKE 'D%')
                DESCRIPTION 'Number of D grades',
            
            f_grade_count AS COUNT_IF(enrollments.final_grade = 'F')
                DESCRIPTION 'Number of F grades'
        )
    );

GRANT REFERENCES ON SEMANTIC VIEW CANVAS_ENROLLMENT_ANALYTICS TO ROLE PUBLIC;
GRANT SELECT ON FGCU_CANVAS_DEMO.CURATED.FACT_ENROLLMENTS TO ROLE PUBLIC;


-- ============================================================================
-- SEMANTIC VIEW 4: SUBMISSION ANALYTICS
-- Business-focused view for assignment submission analysis
-- ============================================================================

CREATE OR REPLACE SEMANTIC VIEW CANVAS_SUBMISSION_ANALYTICS
  COMMENT = 'Assignment submission analytics for Canvas LMS. Use this view to analyze assignment completion rates, grading patterns, and student performance on assessments.'
  AS
    TABLES (
        SUBMISSIONS AS (
            SELECT 
                submission_id,
                assignment_id,
                student_id,
                submission_date,
                score,
                grade,
                late_flag,
                attempt_number,
                graded_at,
                grader_id,
                created_at
            FROM FGCU_CANVAS_DEMO.CURATED.FACT_SUBMISSIONS
        ),
        
        ASSIGNMENTS AS (
            SELECT 
                assignment_id,
                course_id,
                assignment_name,
                assignment_type,
                points_possible,
                due_date,
                status
            FROM FGCU_CANVAS_DEMO.CURATED.DIM_ASSIGNMENTS
        ),
        
        STUDENTS AS (
            SELECT 
                student_id,
                first_name || ' ' || last_name AS student_name,
                major,
                classification
            FROM FGCU_CANVAS_DEMO.CURATED.DIM_STUDENTS
        )
        
        RELATIONSHIPS (
            submissions (assignment_id) REFERENCES assignments (assignment_id),
            submissions (student_id) REFERENCES students (student_id)
        )
        
        FACTS (
            submission_score AS submissions.score
                DESCRIPTION 'Score received on the submission',
            
            max_points AS assignments.points_possible
                DESCRIPTION 'Maximum points possible for the assignment',
            
            attempts AS submissions.attempt_number
                DESCRIPTION 'Number of submission attempts'
        )
        
        DIMENSIONS (
            submission_id AS submissions.submission_id
                DESCRIPTION 'Unique submission identifier'
                PRIMARY KEY,
            
            assignment_id AS submissions.assignment_id
                DESCRIPTION 'Assignment identifier',
            
            assignment_name AS assignments.assignment_name
                DESCRIPTION 'Name of the assignment',
            
            assignment_type AS assignments.assignment_type
                DESCRIPTION 'Type of assignment: Quiz, Homework, Exam, Project, Discussion',
            
            student_id AS submissions.student_id
                DESCRIPTION 'Student identifier',
            
            student_name AS students.student_name
                DESCRIPTION 'Name of the student',
            
            student_major AS students.major
                DESCRIPTION 'Student major',
            
            submission_date AS submissions.submission_date
                DESCRIPTION 'Date and time of submission',
            
            due_date AS assignments.due_date
                DESCRIPTION 'Assignment due date',
            
            is_late AS submissions.late_flag
                DESCRIPTION 'Whether the submission was late',
            
            grade AS submissions.grade
                DESCRIPTION 'Letter grade assigned',
            
            graded_date AS submissions.graded_at
                DESCRIPTION 'Date submission was graded',
            
            score_category AS 
                CASE 
                    WHEN submissions.score >= assignments.points_possible * 0.9 THEN 'Excellent (90%+)'
                    WHEN submissions.score >= assignments.points_possible * 0.8 THEN 'Good (80-89%)'
                    WHEN submissions.score >= assignments.points_possible * 0.7 THEN 'Satisfactory (70-79%)'
                    WHEN submissions.score >= assignments.points_possible * 0.6 THEN 'Below Average (60-69%)'
                    ELSE 'Failing (<60%)'
                END
                DESCRIPTION 'Score category based on percentage'
        )
        
        METRICS (
            total_submissions AS COUNT(submissions.submission_id)
                DESCRIPTION 'Total number of submissions',
            
            unique_students_submitted AS COUNT(DISTINCT submissions.student_id)
                DESCRIPTION 'Number of unique students who submitted',
            
            average_score AS AVG(submissions.score)
                DESCRIPTION 'Average score across submissions',
            
            average_percentage AS 
                AVG(submissions.score * 100.0 / NULLIF(assignments.points_possible, 0))
                DESCRIPTION 'Average percentage score',
            
            late_submission_count AS COUNT_IF(submissions.late_flag = TRUE)
                DESCRIPTION 'Number of late submissions',
            
            late_submission_rate AS 
                COUNT_IF(submissions.late_flag = TRUE) * 100.0 / NULLIF(COUNT(submissions.submission_id), 0)
                DESCRIPTION 'Percentage of submissions that were late',
            
            avg_attempts AS AVG(submissions.attempt_number)
                DESCRIPTION 'Average number of submission attempts',
            
            graded_count AS COUNT_IF(submissions.graded_at IS NOT NULL)
                DESCRIPTION 'Number of graded submissions',
            
            pending_grading AS COUNT_IF(submissions.graded_at IS NULL)
                DESCRIPTION 'Number of submissions awaiting grading'
        )
    );

GRANT REFERENCES ON SEMANTIC VIEW CANVAS_SUBMISSION_ANALYTICS TO ROLE PUBLIC;
GRANT SELECT ON FGCU_CANVAS_DEMO.CURATED.FACT_SUBMISSIONS TO ROLE PUBLIC;
GRANT SELECT ON FGCU_CANVAS_DEMO.CURATED.DIM_ASSIGNMENTS TO ROLE PUBLIC;


-- ============================================================================
-- SEMANTIC VIEW 5: STUDENT PERFORMANCE AGGREGATE
-- Pre-aggregated view for student performance dashboards
-- ============================================================================

CREATE OR REPLACE SEMANTIC VIEW CANVAS_PERFORMANCE_DASHBOARD
  COMMENT = 'Aggregated student performance metrics for dashboards and KPI tracking. Use this view for high-level performance summaries by student, course, and term.'
  AS
    TABLES (
        PERFORMANCE AS (
            SELECT 
                student_id,
                course_id,
                term,
                total_assignments,
                completed_assignments,
                average_score,
                total_points_earned,
                total_points_possible,
                late_submissions,
                on_time_submissions,
                performance_grade,
                last_activity_date,
                calculated_at
            FROM FGCU_CANVAS_DEMO.CURATED.AGG_STUDENT_COURSE_PERFORMANCE
        ),
        
        STUDENTS AS (
            SELECT 
                student_id,
                first_name || ' ' || last_name AS student_name,
                major,
                classification,
                gpa
            FROM FGCU_CANVAS_DEMO.CURATED.DIM_STUDENTS
        ),
        
        COURSES AS (
            SELECT 
                course_id,
                course_code,
                course_name,
                department,
                instructor_name
            FROM FGCU_CANVAS_DEMO.CURATED.DIM_COURSES
        )
        
        RELATIONSHIPS (
            performance (student_id) REFERENCES students (student_id),
            performance (course_id) REFERENCES courses (course_id)
        )
        
        FACTS (
            assignments_total AS performance.total_assignments
                DESCRIPTION 'Total assignments in the course',
            
            assignments_completed AS performance.completed_assignments
                DESCRIPTION 'Number of assignments completed by student',
            
            avg_assignment_score AS performance.average_score
                DESCRIPTION 'Average score on assignments',
            
            points_earned AS performance.total_points_earned
                DESCRIPTION 'Total points earned',
            
            points_possible AS performance.total_points_possible
                DESCRIPTION 'Total points possible',
            
            late_count AS performance.late_submissions
                DESCRIPTION 'Number of late submissions',
            
            ontime_count AS performance.on_time_submissions
                DESCRIPTION 'Number of on-time submissions'
        )
        
        DIMENSIONS (
            student_id AS performance.student_id
                DESCRIPTION 'Student identifier',
            
            student_name AS students.student_name
                DESCRIPTION 'Student full name',
            
            major AS students.major
                DESCRIPTION 'Student major',
            
            classification AS students.classification
                DESCRIPTION 'Student classification',
            
            overall_gpa AS students.gpa
                DESCRIPTION 'Student overall GPA',
            
            course_id AS performance.course_id
                DESCRIPTION 'Course identifier',
            
            course_code AS courses.course_code
                DESCRIPTION 'Course code',
            
            course_name AS courses.course_name
                DESCRIPTION 'Course name',
            
            department AS courses.department
                DESCRIPTION 'Academic department',
            
            instructor AS courses.instructor_name
                DESCRIPTION 'Course instructor',
            
            term AS performance.term
                DESCRIPTION 'Academic term',
            
            course_grade AS performance.performance_grade
                DESCRIPTION 'Current grade in course',
            
            last_active AS performance.last_activity_date
                DESCRIPTION 'Last activity date in course'
        )
        
        METRICS (
            total_records AS COUNT(*)
                DESCRIPTION 'Total student-course combinations',
            
            avg_completion_rate AS 
                AVG(performance.completed_assignments * 100.0 / NULLIF(performance.total_assignments, 0))
                DESCRIPTION 'Average assignment completion rate',
            
            avg_course_score AS AVG(performance.average_score)
                DESCRIPTION 'Average score across all student-course combinations',
            
            total_late_submissions AS SUM(performance.late_submissions)
                DESCRIPTION 'Total late submissions across all',
            
            students_at_risk AS 
                COUNT_IF(performance.average_score < 60)
                DESCRIPTION 'Number of students scoring below 60%',
            
            high_performers AS 
                COUNT_IF(performance.average_score >= 90)
                DESCRIPTION 'Number of students scoring 90% or above'
        )
    );

GRANT REFERENCES ON SEMANTIC VIEW CANVAS_PERFORMANCE_DASHBOARD TO ROLE PUBLIC;
GRANT SELECT ON FGCU_CANVAS_DEMO.CURATED.AGG_STUDENT_COURSE_PERFORMANCE TO ROLE PUBLIC;


-- ============================================================================
-- VERIFICATION
-- ============================================================================

-- Show all created semantic views
SHOW SEMANTIC VIEWS IN SCHEMA FGCU_CANVAS_DEMO.ANALYTICS;

-- Describe a semantic view to see its structure
DESCRIBE SEMANTIC VIEW CANVAS_STUDENT_ANALYTICS;

-- Show dimensions in a semantic view
SHOW SEMANTIC DIMENSIONS IN SEMANTIC VIEW CANVAS_STUDENT_ANALYTICS;

-- Show metrics in a semantic view
SHOW SEMANTIC METRICS IN SEMANTIC VIEW CANVAS_STUDENT_ANALYTICS;

-- Test querying a semantic view
SELECT * FROM CANVAS_STUDENT_ANALYTICS LIMIT 10;

PRINT '✅ Semantic views created successfully!';
PRINT 'Next step: Run 02_cortex_search_setup.sql';

