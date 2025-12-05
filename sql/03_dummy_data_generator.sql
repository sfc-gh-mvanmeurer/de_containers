/*
================================================================================
FGCU Canvas Data Engineering - Synthetic Data Generator
================================================================================
Stored procedures to generate realistic Canvas LMS dummy data for testing.
================================================================================
*/

USE DATABASE FGCU_CANVAS_DEMO;
USE SCHEMA RAW;
USE WAREHOUSE FGCU_TASK_WH;

-- ============================================================================
-- HELPER FUNCTIONS
-- ============================================================================

-- Generate random FGCU student ID (format: U12345678)
CREATE OR REPLACE FUNCTION GENERATE_STUDENT_ID()
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
    'U' || LPAD(UNIFORM(10000000, 99999999, RANDOM())::VARCHAR, 8, '0')
$$;

-- Generate random course code (format: COP-1234)
CREATE OR REPLACE FUNCTION GENERATE_COURSE_CODE(dept VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
    dept || '-' || LPAD(UNIFORM(1000, 4999, RANDOM())::VARCHAR, 4, '0')
$$;

-- ============================================================================
-- STORED PROCEDURE: Generate Student Data
-- ============================================================================

CREATE OR REPLACE PROCEDURE GENERATE_DUMMY_STUDENTS(num_students INTEGER)
RETURNS VARCHAR
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python', 'faker')
HANDLER = 'generate_students'
AS
$$
import json
from faker import Faker
from snowflake.snowpark import Session
import random

def generate_students(session: Session, num_students: int) -> str:
    fake = Faker()
    Faker.seed(random.randint(1, 10000))
    
    majors = [
        'Computer Science', 'Software Engineering', 'Data Science',
        'Information Systems', 'Cybersecurity', 'Computer Engineering',
        'Biology', 'Chemistry', 'Physics', 'Mathematics',
        'Business Administration', 'Marketing', 'Finance', 'Accounting',
        'Psychology', 'Nursing', 'Education', 'Communications',
        'Environmental Science', 'Civil Engineering', 'Mechanical Engineering'
    ]
    
    classifications = ['Freshman', 'Sophomore', 'Junior', 'Senior']
    statuses = ['Active', 'Active', 'Active', 'Active', 'Inactive', 'Probation']
    
    students = []
    for i in range(num_students):
        student_id = f"U{random.randint(10000000, 99999999)}"
        classification = random.choice(classifications)
        
        # Set enrollment year based on classification
        enrollment_years_ago = {'Freshman': 0, 'Sophomore': 1, 'Junior': 2, 'Senior': 3}
        enrollment_year = 2024 - enrollment_years_ago[classification]
        
        student = {
            "student_id": student_id,
            "canvas_user_id": random.randint(100000, 999999),
            "first_name": fake.first_name(),
            "last_name": fake.last_name(),
            "email": f"{student_id.lower()}@eagle.fgcu.edu",
            "major": random.choice(majors),
            "classification": classification,
            "enrollment_status": random.choice(statuses),
            "enrollment_date": f"{enrollment_year}-08-{random.randint(15, 25):02d}",
            "expected_graduation": f"{enrollment_year + 4}-05-15",
            "gpa": round(random.uniform(2.0, 4.0), 2),
            "advisor_id": f"ADV{random.randint(1000, 9999)}"
        }
        students.append(student)
    
    # Insert into RAW_STUDENTS
    for student in students:
        payload_json = json.dumps(student)
        session.sql(f"""
            INSERT INTO RAW_STUDENTS (payload, file_name)
            SELECT PARSE_JSON('{payload_json}'), 'synthetic_data_generator'
        """).collect()
    
    return f"Successfully generated {num_students} student records"
$$;

-- ============================================================================
-- STORED PROCEDURE: Generate Course Data
-- ============================================================================

CREATE OR REPLACE PROCEDURE GENERATE_DUMMY_COURSES(num_courses INTEGER)
RETURNS VARCHAR
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python', 'faker')
HANDLER = 'generate_courses'
AS
$$
import json
from faker import Faker
from snowflake.snowpark import Session
import random

def generate_courses(session: Session, num_courses: int) -> str:
    fake = Faker()
    
    departments = {
        'COP': 'Computer Science',
        'CIS': 'Information Systems', 
        'CAP': 'Applied Computing',
        'CDA': 'Computer Design',
        'CNT': 'Networking',
        'CEN': 'Software Engineering',
        'MAT': 'Mathematics',
        'STA': 'Statistics',
        'PHY': 'Physics',
        'CHM': 'Chemistry',
        'BIO': 'Biology',
        'ACC': 'Accounting',
        'FIN': 'Finance',
        'MAN': 'Management',
        'MAR': 'Marketing'
    }
    
    course_names = {
        'COP': ['Introduction to Programming', 'Data Structures', 'Algorithms', 'Database Systems', 
                'Operating Systems', 'Computer Networks', 'Software Engineering', 'Web Development'],
        'CIS': ['Information Systems Fundamentals', 'Business Intelligence', 'Enterprise Systems',
                'IT Project Management', 'Systems Analysis and Design'],
        'MAT': ['Calculus I', 'Calculus II', 'Linear Algebra', 'Discrete Mathematics', 
                'Differential Equations', 'Numerical Methods'],
        'STA': ['Statistics I', 'Statistics II', 'Probability Theory', 'Data Analysis'],
        'ACC': ['Financial Accounting', 'Managerial Accounting', 'Auditing', 'Tax Accounting'],
        'FIN': ['Corporate Finance', 'Investments', 'Financial Markets', 'Risk Management']
    }
    
    delivery_modes = ['In-Person', 'Online', 'Hybrid']
    terms = ['Fall 2024', 'Spring 2025']
    
    courses = []
    for i in range(num_courses):
        dept_code = random.choice(list(departments.keys()))
        dept_name = departments[dept_code]
        course_num = random.randint(1000, 4999)
        
        # Get appropriate course name or generate one
        if dept_code in course_names:
            name = random.choice(course_names[dept_code])
        else:
            name = f"{dept_name} Topics {course_num}"
        
        term = random.choice(terms)
        year = '2024' if 'Fall' in term else '2025'
        
        course = {
            "course_id": f"CRS{random.randint(100000, 999999)}",
            "canvas_course_id": random.randint(10000, 99999),
            "course_code": f"{dept_code}-{course_num}",
            "course_name": name,
            "department": dept_name,
            "credit_hours": random.choice([1, 2, 3, 3, 3, 4]),
            "course_level": "Undergraduate" if course_num < 5000 else "Graduate",
            "delivery_mode": random.choice(delivery_modes),
            "term": term,
            "academic_year": "2024-2025",
            "instructor_id": f"INS{random.randint(1000, 9999)}",
            "instructor_name": fake.name(),
            "start_date": f"{year}-01-08" if 'Spring' in term else f"{year}-08-19",
            "end_date": f"{year}-05-01" if 'Spring' in term else f"{year}-12-13",
            "max_enrollment": random.choice([25, 30, 35, 40, 50, 100, 200])
        }
        courses.append(course)
    
    # Insert into RAW_COURSES
    for course in courses:
        payload_json = json.dumps(course).replace("'", "''")
        session.sql(f"""
            INSERT INTO RAW_COURSES (payload, file_name)
            SELECT PARSE_JSON($${payload_json}$$), 'synthetic_data_generator'
        """).collect()
    
    return f"Successfully generated {num_courses} course records"
$$;

-- ============================================================================
-- STORED PROCEDURE: Generate Enrollment Data
-- ============================================================================

CREATE OR REPLACE PROCEDURE GENERATE_DUMMY_ENROLLMENTS(enrollments_per_student INTEGER)
RETURNS VARCHAR
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'generate_enrollments'
AS
$$
import json
from snowflake.snowpark import Session
import random
from datetime import datetime, timedelta

def generate_enrollments(session: Session, enrollments_per_student: int) -> str:
    # Get existing students
    students_df = session.sql("""
        SELECT payload:student_id::VARCHAR as student_id 
        FROM RAW_STUDENTS 
        WHERE processing_status != 'ERROR'
    """).collect()
    
    # Get existing courses
    courses_df = session.sql("""
        SELECT payload:course_id::VARCHAR as course_id,
               payload:term::VARCHAR as term
        FROM RAW_COURSES
        WHERE processing_status != 'ERROR'
    """).collect()
    
    if not students_df or not courses_df:
        return "No students or courses found. Generate those first."
    
    student_ids = [row['STUDENT_ID'] for row in students_df]
    course_data = [(row['COURSE_ID'], row['TERM']) for row in courses_df]
    
    enrollment_states = ['active', 'active', 'active', 'active', 'completed', 'dropped']
    
    count = 0
    for student_id in student_ids:
        # Randomly select courses for this student
        selected_courses = random.sample(course_data, min(enrollments_per_student, len(course_data)))
        
        for course_id, term in selected_courses:
            enrollment = {
                "enrollment_id": f"ENR{random.randint(10000000, 99999999)}",
                "student_id": student_id,
                "course_id": course_id,
                "enrollment_state": random.choice(enrollment_states),
                "enrollment_type": "StudentEnrollment",
                "enrolled_at": "2024-08-15T10:00:00Z" if "Fall" in str(term) else "2025-01-06T10:00:00Z",
                "final_grade": random.choice(['A', 'A-', 'B+', 'B', 'B-', 'C+', 'C', 'C-', 'D', 'F', None]),
                "final_score": round(random.uniform(50, 100), 1)
            }
            
            payload_json = json.dumps(enrollment).replace("'", "''")
            session.sql(f"""
                INSERT INTO RAW_ENROLLMENTS (payload, file_name)
                SELECT PARSE_JSON($${payload_json}$$), 'synthetic_data_generator'
            """).collect()
            count += 1
    
    return f"Successfully generated {count} enrollment records"
$$;

-- ============================================================================
-- STORED PROCEDURE: Generate Assignment Data
-- ============================================================================

CREATE OR REPLACE PROCEDURE GENERATE_DUMMY_ASSIGNMENTS(assignments_per_course INTEGER)
RETURNS VARCHAR
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'generate_assignments'
AS
$$
import json
from snowflake.snowpark import Session
import random
from datetime import datetime, timedelta

def generate_assignments(session: Session, assignments_per_course: int) -> str:
    # Get existing courses
    courses_df = session.sql("""
        SELECT payload:course_id::VARCHAR as course_id,
               payload:start_date::DATE as start_date,
               payload:end_date::DATE as end_date
        FROM RAW_COURSES
        WHERE processing_status != 'ERROR'
    """).collect()
    
    if not courses_df:
        return "No courses found. Generate those first."
    
    assignment_types = [
        ('Quiz', 20, 50),
        ('Homework', 10, 25),
        ('Exam', 100, 200),
        ('Project', 50, 150),
        ('Discussion', 10, 20),
        ('Lab', 25, 50)
    ]
    
    count = 0
    for course_row in courses_df:
        course_id = course_row['COURSE_ID']
        
        for i in range(assignments_per_course):
            atype, min_pts, max_pts = random.choice(assignment_types)
            points = random.randint(min_pts, max_pts)
            
            # Random due date within course period
            base_date = datetime(2024, 8, 20)
            due_offset = random.randint(1, 100)
            due_date = base_date + timedelta(days=due_offset)
            
            assignment = {
                "assignment_id": f"ASN{random.randint(10000000, 99999999)}",
                "canvas_assignment_id": random.randint(100000, 999999),
                "course_id": course_id,
                "assignment_name": f"{atype} {i+1}: {random.choice(['Chapter', 'Module', 'Unit', 'Week'])} {random.randint(1, 15)}",
                "assignment_type": atype,
                "points_possible": points,
                "due_date": due_date.strftime("%Y-%m-%dT23:59:00Z"),
                "unlock_date": (due_date - timedelta(days=7)).strftime("%Y-%m-%dT00:00:00Z"),
                "lock_date": (due_date + timedelta(days=3)).strftime("%Y-%m-%dT23:59:00Z"),
                "submission_types": "online_upload,online_text_entry" if atype != "Quiz" else "online_quiz",
                "is_group_assignment": atype == "Project" and random.random() > 0.5,
                "weight": round(random.uniform(5, 25), 1)
            }
            
            payload_json = json.dumps(assignment).replace("'", "''")
            session.sql(f"""
                INSERT INTO RAW_ASSIGNMENTS (payload, file_name)
                SELECT PARSE_JSON($${payload_json}$$), 'synthetic_data_generator'
            """).collect()
            count += 1
    
    return f"Successfully generated {count} assignment records"
$$;

-- ============================================================================
-- STORED PROCEDURE: Generate Submission Data
-- ============================================================================

CREATE OR REPLACE PROCEDURE GENERATE_DUMMY_SUBMISSIONS()
RETURNS VARCHAR
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'generate_submissions'
AS
$$
import json
from snowflake.snowpark import Session
import random
from datetime import datetime, timedelta

def generate_submissions(session: Session) -> str:
    # Get enrollment-assignment pairs
    pairs_df = session.sql("""
        SELECT DISTINCT
            e.payload:student_id::VARCHAR as student_id,
            a.payload:assignment_id::VARCHAR as assignment_id,
            a.payload:points_possible::NUMBER as points_possible,
            a.payload:due_date::TIMESTAMP as due_date
        FROM RAW_ENROLLMENTS e
        JOIN RAW_ASSIGNMENTS a ON e.payload:course_id = a.payload:course_id
        WHERE e.processing_status != 'ERROR' AND a.processing_status != 'ERROR'
        LIMIT 5000
    """).collect()
    
    if not pairs_df:
        return "No enrollment-assignment pairs found. Generate enrollments and assignments first."
    
    grades = ['A', 'A-', 'B+', 'B', 'B-', 'C+', 'C', 'C-', 'D+', 'D', 'F']
    
    count = 0
    for pair in pairs_df:
        # 90% submission rate
        if random.random() > 0.9:
            continue
            
        student_id = pair['STUDENT_ID']
        assignment_id = pair['ASSIGNMENT_ID']
        points_possible = float(pair['POINTS_POSSIBLE'] or 100)
        
        # Calculate score with realistic distribution
        score_pct = random.gauss(0.78, 0.15)  # Mean 78%, std 15%
        score_pct = max(0, min(1, score_pct))  # Clamp to 0-1
        score = round(score_pct * points_possible, 1)
        
        # Determine if late (10% chance)
        is_late = random.random() < 0.1
        
        # Submission timestamp
        base_time = datetime(2024, 10, 15, 23, 45, 0)
        submit_offset = timedelta(hours=random.randint(-48, 2))
        submitted_at = base_time + submit_offset
        
        submission = {
            "submission_id": f"SUB{random.randint(10000000, 99999999)}",
            "student_id": student_id,
            "assignment_id": assignment_id,
            "submitted_at": submitted_at.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "graded_at": (submitted_at + timedelta(days=random.randint(1, 7))).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "score": score,
            "grade": grades[min(int((1 - score_pct) * 10), 10)] if score_pct > 0.1 else 'F',
            "points_possible": points_possible,
            "percentage": round(score_pct * 100, 1),
            "submission_type": random.choice(["online_upload", "online_text_entry"]),
            "attempt_number": random.choice([1, 1, 1, 1, 2, 2, 3]),
            "late_flag": is_late,
            "missing_flag": False,
            "excused_flag": random.random() < 0.02,
            "grader_id": f"GRD{random.randint(1000, 9999)}"
        }
        
        payload_json = json.dumps(submission).replace("'", "''")
        session.sql(f"""
            INSERT INTO RAW_SUBMISSIONS (payload, file_name)
            SELECT PARSE_JSON($${payload_json}$$), 'synthetic_data_generator'
        """).collect()
        count += 1
    
    return f"Successfully generated {count} submission records"
$$;

-- ============================================================================
-- STORED PROCEDURE: Generate Activity Log Data
-- ============================================================================

CREATE OR REPLACE PROCEDURE GENERATE_DUMMY_ACTIVITY_LOGS(logs_per_enrollment INTEGER)
RETURNS VARCHAR
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'generate_activity_logs'
AS
$$
import json
from snowflake.snowpark import Session
import random
from datetime import datetime, timedelta

def generate_activity_logs(session: Session, logs_per_enrollment: int) -> str:
    # Get enrollments
    enrollments_df = session.sql("""
        SELECT 
            payload:student_id::VARCHAR as student_id,
            payload:course_id::VARCHAR as course_id
        FROM RAW_ENROLLMENTS
        WHERE processing_status != 'ERROR'
        LIMIT 1000
    """).collect()
    
    if not enrollments_df:
        return "No enrollments found. Generate those first."
    
    activity_types = ['PageView', 'Assignment', 'Quiz', 'Discussion', 'VideoWatch', 'FileDownload']
    devices = ['Desktop', 'Mobile', 'Tablet']
    browsers = ['Chrome', 'Safari', 'Firefox', 'Edge']
    
    count = 0
    for enrollment in enrollments_df:
        student_id = enrollment['STUDENT_ID']
        course_id = enrollment['COURSE_ID']
        
        for i in range(logs_per_enrollment):
            activity_type = random.choice(activity_types)
            
            # Activity timestamp - random time in semester
            base_date = datetime(2024, 8, 20)
            offset_days = random.randint(0, 100)
            offset_hours = random.randint(6, 23)  # Most activity during waking hours
            activity_time = base_date + timedelta(days=offset_days, hours=offset_hours, 
                                                   minutes=random.randint(0, 59))
            
            activity = {
                "activity_id": f"ACT{random.randint(100000000, 999999999)}",
                "student_id": student_id,
                "course_id": course_id,
                "activity_type": activity_type,
                "activity_timestamp": activity_time.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "duration_seconds": random.randint(10, 3600),
                "page_url": f"/courses/{course_id}/{activity_type.lower()}/{random.randint(1, 100)}",
                "device_type": random.choice(devices),
                "browser": random.choice(browsers),
                "ip_address": f"{random.randint(10, 200)}.{random.randint(0, 255)}.{random.randint(0, 255)}.{random.randint(1, 254)}"
            }
            
            payload_json = json.dumps(activity).replace("'", "''")
            session.sql(f"""
                INSERT INTO RAW_ACTIVITY_LOGS (payload, file_name)
                SELECT PARSE_JSON($${payload_json}$$), 'synthetic_data_generator'
            """).collect()
            count += 1
    
    return f"Successfully generated {count} activity log records"
$$;

-- ============================================================================
-- MASTER PROCEDURE: Generate Complete Dataset
-- ============================================================================

CREATE OR REPLACE PROCEDURE GENERATE_COMPLETE_CANVAS_DATASET(
    num_students INTEGER DEFAULT 100,
    num_courses INTEGER DEFAULT 20,
    enrollments_per_student INTEGER DEFAULT 5,
    assignments_per_course INTEGER DEFAULT 15,
    activity_logs_per_enrollment INTEGER DEFAULT 25
)
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
    result VARCHAR;
BEGIN
    -- Generate students
    CALL GENERATE_DUMMY_STUDENTS(:num_students);
    
    -- Generate courses
    CALL GENERATE_DUMMY_COURSES(:num_courses);
    
    -- Generate enrollments
    CALL GENERATE_DUMMY_ENROLLMENTS(:enrollments_per_student);
    
    -- Generate assignments
    CALL GENERATE_DUMMY_ASSIGNMENTS(:assignments_per_course);
    
    -- Generate submissions
    CALL GENERATE_DUMMY_SUBMISSIONS();
    
    -- Generate activity logs
    CALL GENERATE_DUMMY_ACTIVITY_LOGS(:activity_logs_per_enrollment);
    
    result := 'Complete Canvas dataset generated successfully!';
    RETURN result;
END;
$$;

-- ============================================================================
-- USAGE EXAMPLES
-- ============================================================================

-- Generate a small test dataset
-- CALL GENERATE_COMPLETE_CANVAS_DATASET(50, 10, 4, 10, 15);

-- Generate a larger dataset for demos
-- CALL GENERATE_COMPLETE_CANVAS_DATASET(500, 50, 6, 20, 30);

-- Generate individual data types
-- CALL GENERATE_DUMMY_STUDENTS(100);
-- CALL GENERATE_DUMMY_COURSES(20);

SELECT 'Dummy data generator procedures created successfully!' AS STATUS;



