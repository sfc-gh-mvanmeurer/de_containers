/*
================================================================================
Demo Canvas Data Engineering - Canvas LMS Data Model
================================================================================
This script creates the data model for Canvas LMS student data.
Includes RAW (landing) and CURATED (transformed) layer tables.
================================================================================
*/

USE DATABASE DEMO_CANVAS_DB;
USE WAREHOUSE DEMO_TRANSFORM_WH;

-- ============================================================================
-- RAW LAYER - Landing Zone Tables
-- ============================================================================

USE SCHEMA RAW;

-- Raw Students Table
CREATE OR REPLACE TABLE RAW_STUDENTS (
    raw_id              VARCHAR(36) DEFAULT UUID_STRING(),
    payload             VARIANT,
    source_system       VARCHAR(50) DEFAULT 'CANVAS_LMS',
    file_name           VARCHAR(500),
    ingested_at         TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    processing_status   VARCHAR(20) DEFAULT 'PENDING'
);

-- Raw Courses Table
CREATE OR REPLACE TABLE RAW_COURSES (
    raw_id              VARCHAR(36) DEFAULT UUID_STRING(),
    payload             VARIANT,
    source_system       VARCHAR(50) DEFAULT 'CANVAS_LMS',
    file_name           VARCHAR(500),
    ingested_at         TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    processing_status   VARCHAR(20) DEFAULT 'PENDING'
);

-- Raw Enrollments Table
CREATE OR REPLACE TABLE RAW_ENROLLMENTS (
    raw_id              VARCHAR(36) DEFAULT UUID_STRING(),
    payload             VARIANT,
    source_system       VARCHAR(50) DEFAULT 'CANVAS_LMS',
    file_name           VARCHAR(500),
    ingested_at         TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    processing_status   VARCHAR(20) DEFAULT 'PENDING'
);

-- Raw Assignments Table
CREATE OR REPLACE TABLE RAW_ASSIGNMENTS (
    raw_id              VARCHAR(36) DEFAULT UUID_STRING(),
    payload             VARIANT,
    source_system       VARCHAR(50) DEFAULT 'CANVAS_LMS',
    file_name           VARCHAR(500),
    ingested_at         TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    processing_status   VARCHAR(20) DEFAULT 'PENDING'
);

-- Raw Submissions Table
CREATE OR REPLACE TABLE RAW_SUBMISSIONS (
    raw_id              VARCHAR(36) DEFAULT UUID_STRING(),
    payload             VARIANT,
    source_system       VARCHAR(50) DEFAULT 'CANVAS_LMS',
    file_name           VARCHAR(500),
    ingested_at         TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    processing_status   VARCHAR(20) DEFAULT 'PENDING'
);

-- Raw Grades Table
CREATE OR REPLACE TABLE RAW_GRADES (
    raw_id              VARCHAR(36) DEFAULT UUID_STRING(),
    payload             VARIANT,
    source_system       VARCHAR(50) DEFAULT 'CANVAS_LMS',
    file_name           VARCHAR(500),
    ingested_at         TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    processing_status   VARCHAR(20) DEFAULT 'PENDING'
);

-- Raw Activity Logs Table
CREATE OR REPLACE TABLE RAW_ACTIVITY_LOGS (
    raw_id              VARCHAR(36) DEFAULT UUID_STRING(),
    payload             VARIANT,
    source_system       VARCHAR(50) DEFAULT 'CANVAS_LMS',
    file_name           VARCHAR(500),
    ingested_at         TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    processing_status   VARCHAR(20) DEFAULT 'PENDING'
);

-- ============================================================================
-- CURATED LAYER - Transformed Tables
-- ============================================================================

USE SCHEMA CURATED;

-- Dimension: Students
CREATE OR REPLACE TABLE DIM_STUDENTS (
    student_key         NUMBER AUTOINCREMENT PRIMARY KEY,
    student_id          VARCHAR(20) NOT NULL UNIQUE,
    canvas_user_id      NUMBER,
    first_name          VARCHAR(100),
    last_name           VARCHAR(100),
    email               VARCHAR(200),
    major               VARCHAR(100),
    classification      VARCHAR(20),  -- Freshman, Sophomore, Junior, Senior
    enrollment_status   VARCHAR(20),  -- Active, Inactive, Graduated, Withdrawn
    enrollment_date     DATE,
    expected_graduation DATE,
    gpa                 DECIMAL(3,2),
    advisor_id          VARCHAR(20),
    created_at          TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    updated_at          TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    is_current          BOOLEAN DEFAULT TRUE
);

-- Dimension: Courses
CREATE OR REPLACE TABLE DIM_COURSES (
    course_key          NUMBER AUTOINCREMENT PRIMARY KEY,
    course_id           VARCHAR(20) NOT NULL UNIQUE,
    canvas_course_id    NUMBER,
    course_code         VARCHAR(20),
    course_name         VARCHAR(200),
    department          VARCHAR(100),
    credit_hours        NUMBER(2),
    course_level        VARCHAR(20),  -- Undergraduate, Graduate
    delivery_mode       VARCHAR(30),  -- In-Person, Online, Hybrid
    term                VARCHAR(20),
    academic_year       VARCHAR(10),
    instructor_id       VARCHAR(20),
    instructor_name     VARCHAR(200),
    start_date          DATE,
    end_date            DATE,
    max_enrollment      NUMBER,
    created_at          TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    updated_at          TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    is_current          BOOLEAN DEFAULT TRUE
);

-- Dimension: Assignments
CREATE OR REPLACE TABLE DIM_ASSIGNMENTS (
    assignment_key      NUMBER AUTOINCREMENT PRIMARY KEY,
    assignment_id       VARCHAR(20) NOT NULL UNIQUE,
    canvas_assignment_id NUMBER,
    course_id           VARCHAR(20),
    assignment_name     VARCHAR(300),
    assignment_type     VARCHAR(50),  -- Quiz, Homework, Exam, Project, Discussion
    points_possible     DECIMAL(10,2),
    due_date            TIMESTAMP_NTZ,
    unlock_date         TIMESTAMP_NTZ,
    lock_date           TIMESTAMP_NTZ,
    submission_types    VARCHAR(200),
    is_group_assignment BOOLEAN DEFAULT FALSE,
    weight              DECIMAL(5,2),
    created_at          TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    updated_at          TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Fact: Enrollments
CREATE OR REPLACE TABLE FACT_ENROLLMENTS (
    enrollment_key      NUMBER AUTOINCREMENT PRIMARY KEY,
    enrollment_id       VARCHAR(36) NOT NULL UNIQUE,
    student_key         NUMBER REFERENCES DIM_STUDENTS(student_key),
    course_key          NUMBER REFERENCES DIM_COURSES(course_key),
    student_id          VARCHAR(20),
    course_id           VARCHAR(20),
    enrollment_state    VARCHAR(20),  -- Active, Completed, Dropped, Withdrawn
    enrollment_type     VARCHAR(20),  -- StudentEnrollment, TeacherEnrollment
    enrolled_at         TIMESTAMP_NTZ,
    completed_at        TIMESTAMP_NTZ,
    final_grade         VARCHAR(5),
    final_score         DECIMAL(5,2),
    created_at          TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    updated_at          TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Fact: Submissions
CREATE OR REPLACE TABLE FACT_SUBMISSIONS (
    submission_key      NUMBER AUTOINCREMENT PRIMARY KEY,
    submission_id       VARCHAR(36) NOT NULL UNIQUE,
    student_key         NUMBER REFERENCES DIM_STUDENTS(student_key),
    assignment_key      NUMBER REFERENCES DIM_ASSIGNMENTS(assignment_key),
    student_id          VARCHAR(20),
    assignment_id       VARCHAR(20),
    submitted_at        TIMESTAMP_NTZ,
    graded_at           TIMESTAMP_NTZ,
    score               DECIMAL(10,2),
    grade               VARCHAR(10),
    points_possible     DECIMAL(10,2),
    percentage          DECIMAL(5,2),
    submission_type     VARCHAR(50),
    attempt_number      NUMBER DEFAULT 1,
    late_flag           BOOLEAN DEFAULT FALSE,
    missing_flag        BOOLEAN DEFAULT FALSE,
    excused_flag        BOOLEAN DEFAULT FALSE,
    grader_id           VARCHAR(20),
    created_at          TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    updated_at          TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Fact: Activity Logs (Student Engagement)
CREATE OR REPLACE TABLE FACT_ACTIVITY_LOGS (
    activity_key        NUMBER AUTOINCREMENT PRIMARY KEY,
    activity_id         VARCHAR(36) NOT NULL UNIQUE,
    student_key         NUMBER REFERENCES DIM_STUDENTS(student_key),
    course_key          NUMBER REFERENCES DIM_COURSES(course_key),
    student_id          VARCHAR(20),
    course_id           VARCHAR(20),
    activity_type       VARCHAR(50),  -- PageView, Assignment, Quiz, Discussion, VideoWatch
    activity_timestamp  TIMESTAMP_NTZ,
    duration_seconds    NUMBER,
    page_url            VARCHAR(1000),
    device_type         VARCHAR(30),
    browser             VARCHAR(50),
    ip_address          VARCHAR(45),
    created_at          TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- ============================================================================
-- AGGREGATION TABLES
-- ============================================================================

-- Student Course Performance Summary
CREATE OR REPLACE TABLE AGG_STUDENT_COURSE_PERFORMANCE (
    student_id          VARCHAR(20),
    course_id           VARCHAR(20),
    term                VARCHAR(20),
    total_assignments   NUMBER,
    completed_assignments NUMBER,
    avg_score           DECIMAL(5,2),
    total_points_earned DECIMAL(10,2),
    total_points_possible DECIMAL(10,2),
    late_submissions    NUMBER,
    missing_submissions NUMBER,
    total_activity_minutes NUMBER,
    last_activity_date  DATE,
    current_grade       VARCHAR(5),
    calculated_at       TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Course Analytics Summary
CREATE OR REPLACE TABLE AGG_COURSE_ANALYTICS (
    course_id           VARCHAR(20),
    term                VARCHAR(20),
    total_enrolled      NUMBER,
    active_students     NUMBER,
    avg_class_score     DECIMAL(5,2),
    median_class_score  DECIMAL(5,2),
    grade_distribution  VARIANT,  -- JSON with A/B/C/D/F counts
    completion_rate     DECIMAL(5,2),
    avg_engagement_minutes NUMBER,
    at_risk_students    NUMBER,
    calculated_at       TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- ============================================================================
-- STREAMS FOR CHANGE DATA CAPTURE
-- ============================================================================

USE SCHEMA RAW;

CREATE OR REPLACE STREAM STM_RAW_STUDENTS ON TABLE RAW_STUDENTS;
CREATE OR REPLACE STREAM STM_RAW_COURSES ON TABLE RAW_COURSES;
CREATE OR REPLACE STREAM STM_RAW_ENROLLMENTS ON TABLE RAW_ENROLLMENTS;
CREATE OR REPLACE STREAM STM_RAW_ASSIGNMENTS ON TABLE RAW_ASSIGNMENTS;
CREATE OR REPLACE STREAM STM_RAW_SUBMISSIONS ON TABLE RAW_SUBMISSIONS;
CREATE OR REPLACE STREAM STM_RAW_GRADES ON TABLE RAW_GRADES;
CREATE OR REPLACE STREAM STM_RAW_ACTIVITY_LOGS ON TABLE RAW_ACTIVITY_LOGS;

-- ============================================================================
-- VERIFICATION
-- ============================================================================

SHOW TABLES IN SCHEMA DEMO_CANVAS_DB.RAW;
SHOW TABLES IN SCHEMA DEMO_CANVAS_DB.CURATED;
SHOW STREAMS IN SCHEMA DEMO_CANVAS_DB.RAW;

SELECT 'Canvas data model created successfully!' AS STATUS;



