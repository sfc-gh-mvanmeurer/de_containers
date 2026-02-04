"""
Demo Canvas ETL - Data Ingestion Pipeline
==========================================
Handles processing of raw Canvas LMS data from landing zone to staging.
"""

import logging
from typing import Optional
from datetime import datetime

from snowflake.snowpark import Session
from snowflake.snowpark.functions import col, lit, current_timestamp, parse_json

logger = logging.getLogger(__name__)


class DataIngestionPipeline:
    """
    Data ingestion pipeline for Canvas LMS data.
    Processes raw VARIANT data from landing zone tables.
    """
    
    def __init__(self, session: Session):
        self.session = session
        self.raw_schema = "RAW"
        self.curated_schema = "CURATED"
        self.database = session.get_current_database()
        
    def _get_pending_count(self, table_name: str) -> int:
        """Get count of pending records in raw table."""
        result = self.session.sql(f"""
            SELECT COUNT(*) as cnt 
            FROM {self.database}.{self.raw_schema}.{table_name}
            WHERE processing_status = 'PENDING'
        """).collect()
        return result[0]['CNT'] if result else 0
    
    def _mark_processed(self, table_name: str):
        """Mark all pending records as processed."""
        self.session.sql(f"""
            UPDATE {self.database}.{self.raw_schema}.{table_name}
            SET processing_status = 'PROCESSED'
            WHERE processing_status = 'PENDING'
        """).collect()
    
    def _mark_error(self, table_name: str, error_ids: list):
        """Mark specific records as error."""
        if not error_ids:
            return
        ids_str = ",".join([f"'{id}'" for id in error_ids])
        self.session.sql(f"""
            UPDATE {self.database}.{self.raw_schema}.{table_name}
            SET processing_status = 'ERROR'
            WHERE raw_id IN ({ids_str})
        """).collect()
        
    def process_students(self) -> int:
        """
        Process raw student records from RAW_STUDENTS to DIM_STUDENTS.
        Returns count of records processed.
        """
        logger.info("Processing student data...")
        
        pending_count = self._get_pending_count("RAW_STUDENTS")
        if pending_count == 0:
            logger.info("No pending student records to process")
            return 0
        
        try:
            # Execute MERGE to upsert into dimension table
            # Note: payload is stored as VARCHAR (JSON string), so we use PARSE_JSON()
            self.session.sql(f"""
                MERGE INTO {self.database}.{self.curated_schema}.DIM_STUDENTS tgt
                USING (
                    SELECT 
                        PARSE_JSON(payload):student_id::VARCHAR AS student_id,
                        PARSE_JSON(payload):canvas_user_id::NUMBER AS canvas_user_id,
                        PARSE_JSON(payload):first_name::VARCHAR AS first_name,
                        PARSE_JSON(payload):last_name::VARCHAR AS last_name,
                        PARSE_JSON(payload):email::VARCHAR AS email,
                        PARSE_JSON(payload):major::VARCHAR AS major,
                        PARSE_JSON(payload):classification::VARCHAR AS classification,
                        PARSE_JSON(payload):enrollment_status::VARCHAR AS enrollment_status,
                        PARSE_JSON(payload):enrollment_date::DATE AS enrollment_date,
                        PARSE_JSON(payload):expected_graduation::DATE AS expected_graduation,
                        PARSE_JSON(payload):gpa::DECIMAL(3,2) AS gpa,
                        PARSE_JSON(payload):advisor_id::VARCHAR AS advisor_id
                    FROM {self.database}.{self.raw_schema}.RAW_STUDENTS
                    WHERE processing_status = 'PENDING'
                ) src
                ON tgt.student_id = src.student_id
                WHEN MATCHED THEN UPDATE SET
                    canvas_user_id = src.canvas_user_id,
                    first_name = src.first_name,
                    last_name = src.last_name,
                    email = src.email,
                    major = src.major,
                    classification = src.classification,
                    enrollment_status = src.enrollment_status,
                    gpa = src.gpa,
                    advisor_id = src.advisor_id,
                    updated_at = CURRENT_TIMESTAMP()
                WHEN NOT MATCHED THEN INSERT (
                    student_id, canvas_user_id, first_name, last_name, email,
                    major, classification, enrollment_status, enrollment_date,
                    expected_graduation, gpa, advisor_id
                ) VALUES (
                    src.student_id, src.canvas_user_id, src.first_name, src.last_name, src.email,
                    src.major, src.classification, src.enrollment_status, src.enrollment_date,
                    src.expected_graduation, src.gpa, src.advisor_id
                )
            """).collect()
            
            self._mark_processed("RAW_STUDENTS")
            logger.info(f"Processed {pending_count} student records")
            return pending_count
            
        except Exception as e:
            logger.error(f"Error processing students: {e}")
            raise
            
    def process_courses(self) -> int:
        """
        Process raw course records from RAW_COURSES to DIM_COURSES.
        Returns count of records processed.
        """
        logger.info("Processing course data...")
        
        pending_count = self._get_pending_count("RAW_COURSES")
        if pending_count == 0:
            logger.info("No pending course records to process")
            return 0
        
        try:
            # Note: payload is stored as VARCHAR (JSON string), so we use PARSE_JSON()
            self.session.sql(f"""
                MERGE INTO {self.database}.{self.curated_schema}.DIM_COURSES tgt
                USING (
                    SELECT 
                        PARSE_JSON(payload):course_id::VARCHAR AS course_id,
                        PARSE_JSON(payload):canvas_course_id::NUMBER AS canvas_course_id,
                        PARSE_JSON(payload):course_code::VARCHAR AS course_code,
                        PARSE_JSON(payload):course_name::VARCHAR AS course_name,
                        PARSE_JSON(payload):department::VARCHAR AS department,
                        PARSE_JSON(payload):credit_hours::NUMBER AS credit_hours,
                        PARSE_JSON(payload):course_level::VARCHAR AS course_level,
                        PARSE_JSON(payload):delivery_mode::VARCHAR AS delivery_mode,
                        PARSE_JSON(payload):term::VARCHAR AS term,
                        PARSE_JSON(payload):academic_year::VARCHAR AS academic_year,
                        PARSE_JSON(payload):instructor_id::VARCHAR AS instructor_id,
                        PARSE_JSON(payload):instructor_name::VARCHAR AS instructor_name,
                        PARSE_JSON(payload):start_date::DATE AS start_date,
                        PARSE_JSON(payload):end_date::DATE AS end_date,
                        PARSE_JSON(payload):max_enrollment::NUMBER AS max_enrollment
                    FROM {self.database}.{self.raw_schema}.RAW_COURSES
                    WHERE processing_status = 'PENDING'
                ) src
                ON tgt.course_id = src.course_id
                WHEN MATCHED THEN UPDATE SET
                    canvas_course_id = src.canvas_course_id,
                    course_code = src.course_code,
                    course_name = src.course_name,
                    department = src.department,
                    credit_hours = src.credit_hours,
                    course_level = src.course_level,
                    delivery_mode = src.delivery_mode,
                    term = src.term,
                    academic_year = src.academic_year,
                    instructor_id = src.instructor_id,
                    instructor_name = src.instructor_name,
                    start_date = src.start_date,
                    end_date = src.end_date,
                    max_enrollment = src.max_enrollment,
                    updated_at = CURRENT_TIMESTAMP()
                WHEN NOT MATCHED THEN INSERT (
                    course_id, canvas_course_id, course_code, course_name, department,
                    credit_hours, course_level, delivery_mode, term, academic_year,
                    instructor_id, instructor_name, start_date, end_date, max_enrollment
                ) VALUES (
                    src.course_id, src.canvas_course_id, src.course_code, src.course_name, src.department,
                    src.credit_hours, src.course_level, src.delivery_mode, src.term, src.academic_year,
                    src.instructor_id, src.instructor_name, src.start_date, src.end_date, src.max_enrollment
                )
            """).collect()
            
            self._mark_processed("RAW_COURSES")
            logger.info(f"Processed {pending_count} course records")
            return pending_count
            
        except Exception as e:
            logger.error(f"Error processing courses: {e}")
            raise
            
    def process_enrollments(self) -> int:
        """
        Process raw enrollment records from RAW_ENROLLMENTS to FACT_ENROLLMENTS.
        Returns count of records processed.
        """
        logger.info("Processing enrollment data...")
        
        pending_count = self._get_pending_count("RAW_ENROLLMENTS")
        if pending_count == 0:
            logger.info("No pending enrollment records to process")
            return 0
        
        try:
            # Note: payload is stored as VARCHAR (JSON string), so we use PARSE_JSON()
            self.session.sql(f"""
                MERGE INTO {self.database}.{self.curated_schema}.FACT_ENROLLMENTS tgt
                USING (
                    SELECT 
                        PARSE_JSON(r.payload):enrollment_id::VARCHAR AS enrollment_id,
                        s.student_key,
                        c.course_key,
                        PARSE_JSON(r.payload):student_id::VARCHAR AS student_id,
                        PARSE_JSON(r.payload):course_id::VARCHAR AS course_id,
                        PARSE_JSON(r.payload):enrollment_state::VARCHAR AS enrollment_state,
                        PARSE_JSON(r.payload):enrollment_type::VARCHAR AS enrollment_type,
                        PARSE_JSON(r.payload):enrolled_at::TIMESTAMP_NTZ AS enrolled_at,
                        PARSE_JSON(r.payload):completed_at::TIMESTAMP_NTZ AS completed_at,
                        PARSE_JSON(r.payload):final_grade::VARCHAR AS final_grade,
                        PARSE_JSON(r.payload):final_score::DECIMAL(5,2) AS final_score
                    FROM {self.database}.{self.raw_schema}.RAW_ENROLLMENTS r
                    LEFT JOIN {self.database}.{self.curated_schema}.DIM_STUDENTS s 
                        ON PARSE_JSON(r.payload):student_id::VARCHAR = s.student_id
                    LEFT JOIN {self.database}.{self.curated_schema}.DIM_COURSES c 
                        ON PARSE_JSON(r.payload):course_id::VARCHAR = c.course_id
                    WHERE r.processing_status = 'PENDING'
                ) src
                ON tgt.enrollment_id = src.enrollment_id
                WHEN MATCHED THEN UPDATE SET
                    enrollment_state = src.enrollment_state,
                    completed_at = src.completed_at,
                    final_grade = src.final_grade,
                    final_score = src.final_score,
                    updated_at = CURRENT_TIMESTAMP()
                WHEN NOT MATCHED THEN INSERT (
                    enrollment_id, student_key, course_key, student_id, course_id,
                    enrollment_state, enrollment_type, enrolled_at, completed_at,
                    final_grade, final_score
                ) VALUES (
                    src.enrollment_id, src.student_key, src.course_key, src.student_id, src.course_id,
                    src.enrollment_state, src.enrollment_type, src.enrolled_at, src.completed_at,
                    src.final_grade, src.final_score
                )
            """).collect()
            
            self._mark_processed("RAW_ENROLLMENTS")
            logger.info(f"Processed {pending_count} enrollment records")
            return pending_count
            
        except Exception as e:
            logger.error(f"Error processing enrollments: {e}")
            raise
            
    def process_submissions(self) -> int:
        """
        Process raw submission records from RAW_SUBMISSIONS to FACT_SUBMISSIONS.
        Returns count of records processed.
        """
        logger.info("Processing submission data...")
        
        pending_count = self._get_pending_count("RAW_SUBMISSIONS")
        if pending_count == 0:
            logger.info("No pending submission records to process")
            return 0
        
        try:
            # Note: payload is stored as VARCHAR (JSON string), so we use PARSE_JSON()
            self.session.sql(f"""
                MERGE INTO {self.database}.{self.curated_schema}.FACT_SUBMISSIONS tgt
                USING (
                    SELECT 
                        PARSE_JSON(r.payload):submission_id::VARCHAR AS submission_id,
                        s.student_key,
                        a.assignment_key,
                        PARSE_JSON(r.payload):student_id::VARCHAR AS student_id,
                        PARSE_JSON(r.payload):assignment_id::VARCHAR AS assignment_id,
                        PARSE_JSON(r.payload):submitted_at::TIMESTAMP_NTZ AS submitted_at,
                        PARSE_JSON(r.payload):graded_at::TIMESTAMP_NTZ AS graded_at,
                        PARSE_JSON(r.payload):score::DECIMAL(10,2) AS score,
                        PARSE_JSON(r.payload):grade::VARCHAR AS grade,
                        PARSE_JSON(r.payload):points_possible::DECIMAL(10,2) AS points_possible,
                        PARSE_JSON(r.payload):percentage::DECIMAL(5,2) AS percentage,
                        PARSE_JSON(r.payload):submission_type::VARCHAR AS submission_type,
                        PARSE_JSON(r.payload):attempt_number::NUMBER AS attempt_number,
                        PARSE_JSON(r.payload):late_flag::BOOLEAN AS late_flag,
                        PARSE_JSON(r.payload):missing_flag::BOOLEAN AS missing_flag,
                        PARSE_JSON(r.payload):excused_flag::BOOLEAN AS excused_flag,
                        PARSE_JSON(r.payload):grader_id::VARCHAR AS grader_id
                    FROM {self.database}.{self.raw_schema}.RAW_SUBMISSIONS r
                    LEFT JOIN {self.database}.{self.curated_schema}.DIM_STUDENTS s 
                        ON PARSE_JSON(r.payload):student_id::VARCHAR = s.student_id
                    LEFT JOIN {self.database}.{self.curated_schema}.DIM_ASSIGNMENTS a 
                        ON PARSE_JSON(r.payload):assignment_id::VARCHAR = a.assignment_id
                    WHERE r.processing_status = 'PENDING'
                ) src
                ON tgt.submission_id = src.submission_id
                WHEN MATCHED THEN UPDATE SET
                    graded_at = src.graded_at,
                    score = src.score,
                    grade = src.grade,
                    percentage = src.percentage,
                    late_flag = src.late_flag,
                    missing_flag = src.missing_flag,
                    excused_flag = src.excused_flag,
                    grader_id = src.grader_id,
                    updated_at = CURRENT_TIMESTAMP()
                WHEN NOT MATCHED THEN INSERT (
                    submission_id, student_key, assignment_key, student_id, assignment_id,
                    submitted_at, graded_at, score, grade, points_possible, percentage,
                    submission_type, attempt_number, late_flag, missing_flag, excused_flag, grader_id
                ) VALUES (
                    src.submission_id, src.student_key, src.assignment_key, src.student_id, src.assignment_id,
                    src.submitted_at, src.graded_at, src.score, src.grade, src.points_possible, src.percentage,
                    src.submission_type, src.attempt_number, src.late_flag, src.missing_flag, src.excused_flag, src.grader_id
                )
            """).collect()
            
            self._mark_processed("RAW_SUBMISSIONS")
            logger.info(f"Processed {pending_count} submission records")
            return pending_count
            
        except Exception as e:
            logger.error(f"Error processing submissions: {e}")
            raise
            
    def process_activity_logs(self) -> int:
        """
        Process raw activity log records from RAW_ACTIVITY_LOGS to FACT_ACTIVITY_LOGS.
        Returns count of records processed.
        """
        logger.info("Processing activity log data...")
        
        pending_count = self._get_pending_count("RAW_ACTIVITY_LOGS")
        if pending_count == 0:
            logger.info("No pending activity log records to process")
            return 0
        
        try:
            # Note: payload is stored as VARCHAR (JSON string), so we use PARSE_JSON()
            self.session.sql(f"""
                INSERT INTO {self.database}.{self.curated_schema}.FACT_ACTIVITY_LOGS (
                    activity_id, student_key, course_key, student_id, course_id,
                    activity_type, activity_timestamp, duration_seconds,
                    page_url, device_type, browser, ip_address
                )
                SELECT 
                    PARSE_JSON(r.payload):activity_id::VARCHAR,
                    s.student_key,
                    c.course_key,
                    PARSE_JSON(r.payload):student_id::VARCHAR,
                    PARSE_JSON(r.payload):course_id::VARCHAR,
                    PARSE_JSON(r.payload):activity_type::VARCHAR,
                    PARSE_JSON(r.payload):activity_timestamp::TIMESTAMP_NTZ,
                    PARSE_JSON(r.payload):duration_seconds::NUMBER,
                    PARSE_JSON(r.payload):page_url::VARCHAR,
                    PARSE_JSON(r.payload):device_type::VARCHAR,
                    PARSE_JSON(r.payload):browser::VARCHAR,
                    PARSE_JSON(r.payload):ip_address::VARCHAR
                FROM {self.database}.{self.raw_schema}.RAW_ACTIVITY_LOGS r
                LEFT JOIN {self.database}.{self.curated_schema}.DIM_STUDENTS s 
                    ON PARSE_JSON(r.payload):student_id::VARCHAR = s.student_id
                LEFT JOIN {self.database}.{self.curated_schema}.DIM_COURSES c 
                    ON PARSE_JSON(r.payload):course_id::VARCHAR = c.course_id
                WHERE r.processing_status = 'PENDING'
            """).collect()
            
            self._mark_processed("RAW_ACTIVITY_LOGS")
            logger.info(f"Processed {pending_count} activity log records")
            return pending_count
            
        except Exception as e:
            logger.error(f"Error processing activity logs: {e}")
            raise
            
    def process_all_raw_data(self) -> int:
        """
        Process all raw data tables.
        Returns total count of records processed.
        """
        logger.info("Starting full raw data processing...")
        total = 0
        
        total += self.process_students()
        total += self.process_courses()
        total += self.process_enrollments()
        total += self.process_submissions()
        total += self.process_activity_logs()
        
        logger.info(f"Full raw data processing complete. Total records: {total}")
        return total
        
    def process_incremental(self) -> int:
        """
        Process only pending records (incremental processing).
        Same as process_all_raw_data but more semantically clear.
        """
        return self.process_all_raw_data()



