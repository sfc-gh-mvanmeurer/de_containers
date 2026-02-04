/*
================================================================================
Demo Canvas Data Engineering - Synthetic Data Generator
================================================================================
Stored procedures to generate realistic Canvas LMS dummy data for testing.
================================================================================
*/

USE DATABASE DEMO_CANVAS_DB;
USE SCHEMA RAW;
USE WAREHOUSE DEMO_TASK_WH;

-- ============================================================================
-- HELPER FUNCTIONS
-- ============================================================================

-- Generate random Demo student ID (format: U12345678)
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
        "Computer Science", "Software Engineering", "Data Science",
        "Information Systems", "Cybersecurity", "Computer Engineering",
        "Biology", "Chemistry", "Physics", "Mathematics",
        "Business Administration", "Marketing", "Finance", "Accounting",
        "Psychology", "Nursing", "Education", "Communications",
        "Environmental Science", "Civil Engineering", "Mechanical Engineering"
    ]
    
    classifications = ["Freshman", "Sophomore", "Junior", "Senior"]
    statuses = ["Active", "Active", "Active", "Active", "Inactive", "Probation"]
    
    count = 0
    for i in range(num_students):
        student_id = "U" + str(random.randint(10000000, 99999999))
        classification = random.choice(classifications)
        
        enrollment_years_ago = {"Freshman": 0, "Sophomore": 1, "Junior": 2, "Senior": 3}
        enrollment_year = 2024 - enrollment_years_ago[classification]
        
        student = {
            "student_id": student_id,
            "canvas_user_id": random.randint(100000, 999999),
            "first_name": fake.first_name(),
            "last_name": fake.last_name(),
            "email": student_id.lower() + "@demo-university.edu",
            "major": random.choice(majors),
            "classification": classification,
            "enrollment_status": random.choice(statuses),
            "enrollment_date": str(enrollment_year) + "-08-" + str(random.randint(15, 25)).zfill(2),
            "expected_graduation": str(enrollment_year + 4) + "-05-15",
            "gpa": round(random.uniform(2.0, 4.0), 2),
            "advisor_id": "ADV" + str(random.randint(1000, 9999))
        }
        
        # Use Snowpark DataFrame API to insert
        df = session.create_dataframe([[json.dumps(student), "synthetic_data_generator"]], 
                                       schema=["payload_str", "file_name"])
        df.select(
            df["payload_str"].cast("VARIANT").alias("payload"),
            df["file_name"]
        ).write.mode("append").save_as_table("RAW_STUDENTS", column_order="name")
        count += 1
    
    return "Successfully generated " + str(count) + " student records"
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
        "COP": "Computer Science",
        "CIS": "Information Systems", 
        "CAP": "Applied Computing",
        "CDA": "Computer Design",
        "CNT": "Networking",
        "CEN": "Software Engineering",
        "MAT": "Mathematics",
        "STA": "Statistics",
        "PHY": "Physics",
        "CHM": "Chemistry",
        "BIO": "Biology",
        "ACC": "Accounting",
        "FIN": "Finance",
        "MAN": "Management",
        "MAR": "Marketing"
    }
    
    course_names = {
        "COP": ["Introduction to Programming", "Data Structures", "Algorithms", "Database Systems", 
                "Operating Systems", "Computer Networks", "Software Engineering", "Web Development"],
        "CIS": ["Information Systems Fundamentals", "Business Intelligence", "Enterprise Systems",
                "IT Project Management", "Systems Analysis and Design"],
        "MAT": ["Calculus I", "Calculus II", "Linear Algebra", "Discrete Mathematics", 
                "Differential Equations", "Numerical Methods"],
        "STA": ["Statistics I", "Statistics II", "Probability Theory", "Data Analysis"],
        "ACC": ["Financial Accounting", "Managerial Accounting", "Auditing", "Tax Accounting"],
        "FIN": ["Corporate Finance", "Investments", "Financial Markets", "Risk Management"]
    }
    
    delivery_modes = ["In-Person", "Online", "Hybrid"]
    terms = ["Fall 2024", "Spring 2025"]
    
    count = 0
    for i in range(num_courses):
        dept_code = random.choice(list(departments.keys()))
        dept_name = departments[dept_code]
        course_num = random.randint(1000, 4999)
        
        if dept_code in course_names:
            name = random.choice(course_names[dept_code])
        else:
            name = dept_name + " Topics " + str(course_num)
        
        term = random.choice(terms)
        year = "2024" if "Fall" in term else "2025"
        
        course = {
            "course_id": "CRS" + str(random.randint(100000, 999999)),
            "canvas_course_id": random.randint(10000, 99999),
            "course_code": dept_code + "-" + str(course_num),
            "course_name": name,
            "department": dept_name,
            "credit_hours": random.choice([1, 2, 3, 3, 3, 4]),
            "course_level": "Undergraduate" if course_num < 5000 else "Graduate",
            "delivery_mode": random.choice(delivery_modes),
            "term": term,
            "academic_year": "2024-2025",
            "instructor_id": "INS" + str(random.randint(1000, 9999)),
            "instructor_name": fake.name(),
            "start_date": year + "-01-08" if "Spring" in term else year + "-08-19",
            "end_date": year + "-05-01" if "Spring" in term else year + "-12-13",
            "max_enrollment": random.choice([25, 30, 35, 40, 50, 100, 200])
        }
        
        df = session.create_dataframe([[json.dumps(course), "synthetic_data_generator"]], 
                                       schema=["payload_str", "file_name"])
        df.select(
            df["payload_str"].cast("VARIANT").alias("payload"),
            df["file_name"]
        ).write.mode("append").save_as_table("RAW_COURSES", column_order="name")
        count += 1
    
    return "Successfully generated " + str(count) + " course records"
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
from snowflake.snowpark.functions import parse_json, lit
import random

def generate_enrollments(session: Session, enrollments_per_student: int) -> str:
    students_df = session.sql(
        "SELECT PARSE_JSON(payload):student_id::VARCHAR as student_id FROM RAW_STUDENTS WHERE processing_status != 'ERROR'"
    ).collect()
    
    courses_df = session.sql(
        "SELECT PARSE_JSON(payload):course_id::VARCHAR as course_id, PARSE_JSON(payload):term::VARCHAR as term FROM RAW_COURSES WHERE processing_status != 'ERROR'"
    ).collect()
    
    if not students_df or not courses_df:
        return "No students or courses found. Generate those first."
    
    student_ids = [row["STUDENT_ID"] for row in students_df]
    course_data = [(row["COURSE_ID"], row["TERM"]) for row in courses_df]
    
    enrollment_states = ["active", "active", "active", "active", "completed", "dropped"]
    grades = ["A", "A-", "B+", "B", "B-", "C+", "C", "C-", "D", "F", None]
    
    count = 0
    for student_id in student_ids:
        selected_courses = random.sample(course_data, min(enrollments_per_student, len(course_data)))
        
        for course_id, term in selected_courses:
            enrollment = {
                "enrollment_id": "ENR" + str(random.randint(10000000, 99999999)),
                "student_id": student_id,
                "course_id": course_id,
                "enrollment_state": random.choice(enrollment_states),
                "enrollment_type": "StudentEnrollment",
                "enrolled_at": "2024-08-15T10:00:00Z" if term and "Fall" in str(term) else "2025-01-06T10:00:00Z",
                "final_grade": random.choice(grades),
                "final_score": round(random.uniform(50, 100), 1)
            }
            
            df = session.create_dataframe([[json.dumps(enrollment), "synthetic_data_generator"]], 
                                           schema=["payload_str", "file_name"])
            df.select(
                df["payload_str"].cast("VARIANT").alias("payload"),
                df["file_name"]
            ).write.mode("append").save_as_table("RAW_ENROLLMENTS", column_order="name")
            count += 1
    
    return "Successfully generated " + str(count) + " enrollment records"
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
    courses_df = session.sql(
        "SELECT PARSE_JSON(payload):course_id::VARCHAR as course_id FROM RAW_COURSES WHERE processing_status != 'ERROR'"
    ).collect()
    
    if not courses_df:
        return "No courses found. Generate those first."
    
    assignment_types = [
        ("Quiz", 20, 50),
        ("Homework", 10, 25),
        ("Exam", 100, 200),
        ("Project", 50, 150),
        ("Discussion", 10, 20),
        ("Lab", 25, 50)
    ]
    
    topic_names = ["Chapter", "Module", "Unit", "Week"]
    
    count = 0
    for course_row in courses_df:
        course_id = course_row["COURSE_ID"]
        
        for i in range(assignments_per_course):
            atype, min_pts, max_pts = random.choice(assignment_types)
            points = random.randint(min_pts, max_pts)
            
            base_date = datetime(2024, 8, 20)
            due_offset = random.randint(1, 100)
            due_date = base_date + timedelta(days=due_offset)
            
            assignment = {
                "assignment_id": "ASN" + str(random.randint(10000000, 99999999)),
                "canvas_assignment_id": random.randint(100000, 999999),
                "course_id": course_id,
                "assignment_name": atype + " " + str(i+1) + ": " + random.choice(topic_names) + " " + str(random.randint(1, 15)),
                "assignment_type": atype,
                "points_possible": points,
                "due_date": due_date.strftime("%Y-%m-%dT23:59:00Z"),
                "unlock_date": (due_date - timedelta(days=7)).strftime("%Y-%m-%dT00:00:00Z"),
                "lock_date": (due_date + timedelta(days=3)).strftime("%Y-%m-%dT23:59:00Z"),
                "submission_types": "online_upload,online_text_entry" if atype != "Quiz" else "online_quiz",
                "is_group_assignment": atype == "Project" and random.random() > 0.5,
                "weight": round(random.uniform(5, 25), 1)
            }
            
            df = session.create_dataframe([[json.dumps(assignment), "synthetic_data_generator"]], 
                                           schema=["payload_str", "file_name"])
            df.select(
                df["payload_str"].cast("VARIANT").alias("payload"),
                df["file_name"]
            ).write.mode("append").save_as_table("RAW_ASSIGNMENTS", column_order="name")
            count += 1
    
    return "Successfully generated " + str(count) + " assignment records"
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
    pairs_df = session.sql("""
        SELECT DISTINCT
            PARSE_JSON(e.payload):student_id::VARCHAR as student_id,
            PARSE_JSON(a.payload):assignment_id::VARCHAR as assignment_id,
            PARSE_JSON(a.payload):points_possible::NUMBER as points_possible
        FROM RAW_ENROLLMENTS e
        JOIN RAW_ASSIGNMENTS a ON PARSE_JSON(e.payload):course_id = PARSE_JSON(a.payload):course_id
        WHERE e.processing_status != 'ERROR' AND a.processing_status != 'ERROR'
        LIMIT 5000
    """).collect()
    
    if not pairs_df:
        return "No enrollment-assignment pairs found. Generate enrollments and assignments first."
    
    grades = ["A", "A-", "B+", "B", "B-", "C+", "C", "C-", "D+", "D", "F"]
    
    count = 0
    for pair in pairs_df:
        if random.random() > 0.9:
            continue
            
        student_id = pair["STUDENT_ID"]
        assignment_id = pair["ASSIGNMENT_ID"]
        points_possible = float(pair["POINTS_POSSIBLE"] or 100)
        
        score_pct = random.gauss(0.78, 0.15)
        score_pct = max(0, min(1, score_pct))
        score = round(score_pct * points_possible, 1)
        
        is_late = random.random() < 0.1
        
        base_time = datetime(2024, 10, 15, 23, 45, 0)
        submit_offset = timedelta(hours=random.randint(-48, 2))
        submitted_at = base_time + submit_offset
        
        grade_idx = min(int((1 - score_pct) * 10), 10)
        grade = grades[grade_idx] if score_pct > 0.1 else "F"
        
        submission = {
            "submission_id": "SUB" + str(random.randint(10000000, 99999999)),
            "student_id": student_id,
            "assignment_id": assignment_id,
            "submitted_at": submitted_at.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "graded_at": (submitted_at + timedelta(days=random.randint(1, 7))).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "score": score,
            "grade": grade,
            "points_possible": points_possible,
            "percentage": round(score_pct * 100, 1),
            "submission_type": random.choice(["online_upload", "online_text_entry"]),
            "attempt_number": random.choice([1, 1, 1, 1, 2, 2, 3]),
            "late_flag": is_late,
            "missing_flag": False,
            "excused_flag": random.random() < 0.02,
            "grader_id": "GRD" + str(random.randint(1000, 9999))
        }
        
        df = session.create_dataframe([[json.dumps(submission), "synthetic_data_generator"]], 
                                       schema=["payload_str", "file_name"])
        df.select(
            df["payload_str"].cast("VARIANT").alias("payload"),
            df["file_name"]
        ).write.mode("append").save_as_table("RAW_SUBMISSIONS", column_order="name")
        count += 1
    
    return "Successfully generated " + str(count) + " submission records"
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
    enrollments_df = session.sql("""
        SELECT 
            PARSE_JSON(payload):student_id::VARCHAR as student_id,
            PARSE_JSON(payload):course_id::VARCHAR as course_id
        FROM RAW_ENROLLMENTS
        WHERE processing_status != 'ERROR'
        LIMIT 1000
    """).collect()
    
    if not enrollments_df:
        return "No enrollments found. Generate those first."
    
    activity_types = ["PageView", "Assignment", "Quiz", "Discussion", "VideoWatch", "FileDownload"]
    devices = ["Desktop", "Mobile", "Tablet"]
    browsers = ["Chrome", "Safari", "Firefox", "Edge"]
    
    count = 0
    for enrollment in enrollments_df:
        student_id = enrollment["STUDENT_ID"]
        course_id = enrollment["COURSE_ID"]
        
        for i in range(logs_per_enrollment):
            activity_type = random.choice(activity_types)
            
            base_date = datetime(2024, 8, 20)
            offset_days = random.randint(0, 100)
            offset_hours = random.randint(6, 23)
            activity_time = base_date + timedelta(days=offset_days, hours=offset_hours, 
                                                   minutes=random.randint(0, 59))
            
            activity = {
                "activity_id": "ACT" + str(random.randint(100000000, 999999999)),
                "student_id": student_id,
                "course_id": course_id,
                "activity_type": activity_type,
                "activity_timestamp": activity_time.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "duration_seconds": random.randint(10, 3600),
                "page_url": "/courses/" + str(course_id) + "/" + activity_type.lower() + "/" + str(random.randint(1, 100)),
                "device_type": random.choice(devices),
                "browser": random.choice(browsers),
                "ip_address": str(random.randint(10, 200)) + "." + str(random.randint(0, 255)) + "." + str(random.randint(0, 255)) + "." + str(random.randint(1, 254))
            }
            
            df = session.create_dataframe([[json.dumps(activity), "synthetic_data_generator"]], 
                                           schema=["payload_str", "file_name"])
            df.select(
                df["payload_str"].cast("VARIANT").alias("payload"),
                df["file_name"]
            ).write.mode("append").save_as_table("RAW_ACTIVITY_LOGS", column_order="name")
            count += 1
    
    return "Successfully generated " + str(count) + " activity log records"
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
