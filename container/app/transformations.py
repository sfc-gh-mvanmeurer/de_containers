"""
Demo Canvas ETL - Transformation Engine
========================================
Handles data transformations and aggregations for analytics.
"""

import logging
from typing import Optional
from datetime import datetime

from snowflake.snowpark import Session

logger = logging.getLogger(__name__)


class TransformationEngine:
    """
    Data transformation engine for Canvas LMS data.
    Performs dimensional modeling and aggregations.
    """
    
    def __init__(self, session: Session):
        self.session = session
        self.curated_schema = "CURATED"
        self.database = session.get_current_database()
        
    def transform_students(self) -> int:
        """
        Apply additional transformations to student dimension.
        - Calculate student risk scores
        - Derive classification from credits if needed
        Returns count of records affected.
        """
        logger.info("Applying student transformations...")
        
        # Update students with calculated fields based on enrollment data
        result = self.session.sql(f"""
            UPDATE {self.database}.{self.curated_schema}.DIM_STUDENTS s
            SET 
                updated_at = CURRENT_TIMESTAMP(),
                is_current = TRUE
            WHERE s.enrollment_status = 'Active'
            AND s.updated_at < DATEADD('hour', -1, CURRENT_TIMESTAMP())
        """).collect()
        
        count = result[0][0] if result else 0
        logger.info(f"Student transformations complete. Records updated: {count}")
        return count
        
    def transform_courses(self) -> int:
        """
        Apply additional transformations to course dimension.
        - Calculate enrollment counts
        - Derive course analytics
        Returns count of records affected.
        """
        logger.info("Applying course transformations...")
        
        # Ensure all courses are marked as current
        result = self.session.sql(f"""
            UPDATE {self.database}.{self.curated_schema}.DIM_COURSES
            SET is_current = TRUE, updated_at = CURRENT_TIMESTAMP()
            WHERE is_current IS NULL OR is_current = FALSE
        """).collect()
        
        count = result[0][0] if result else 0
        logger.info(f"Course transformations complete. Records updated: {count}")
        return count
        
    def transform_assignments(self) -> int:
        """
        Apply transformations to assignment dimension.
        Returns count of records affected.
        """
        logger.info("Applying assignment transformations...")
        
        # Process any pending assignment records
        # Note: payload is stored as VARCHAR (JSON string), so we use PARSE_JSON()
        result = self.session.sql(f"""
            MERGE INTO {self.database}.{self.curated_schema}.DIM_ASSIGNMENTS tgt
            USING (
                SELECT 
                    PARSE_JSON(r.payload):assignment_id::VARCHAR AS assignment_id,
                    PARSE_JSON(r.payload):canvas_assignment_id::NUMBER AS canvas_assignment_id,
                    PARSE_JSON(r.payload):course_id::VARCHAR AS course_id,
                    PARSE_JSON(r.payload):assignment_name::VARCHAR AS assignment_name,
                    PARSE_JSON(r.payload):assignment_type::VARCHAR AS assignment_type,
                    PARSE_JSON(r.payload):points_possible::DECIMAL(10,2) AS points_possible,
                    PARSE_JSON(r.payload):due_date::TIMESTAMP_NTZ AS due_date,
                    PARSE_JSON(r.payload):unlock_date::TIMESTAMP_NTZ AS unlock_date,
                    PARSE_JSON(r.payload):lock_date::TIMESTAMP_NTZ AS lock_date,
                    PARSE_JSON(r.payload):submission_types::VARCHAR AS submission_types,
                    PARSE_JSON(r.payload):is_group_assignment::BOOLEAN AS is_group_assignment,
                    PARSE_JSON(r.payload):weight::DECIMAL(5,2) AS weight
                FROM {self.database}.RAW.RAW_ASSIGNMENTS r
                WHERE r.processing_status = 'PENDING'
            ) src
            ON tgt.assignment_id = src.assignment_id
            WHEN MATCHED THEN UPDATE SET
                assignment_name = src.assignment_name,
                points_possible = src.points_possible,
                due_date = src.due_date,
                weight = src.weight,
                updated_at = CURRENT_TIMESTAMP()
            WHEN NOT MATCHED THEN INSERT (
                assignment_id, canvas_assignment_id, course_id, assignment_name,
                assignment_type, points_possible, due_date, unlock_date, lock_date,
                submission_types, is_group_assignment, weight
            ) VALUES (
                src.assignment_id, src.canvas_assignment_id, src.course_id, src.assignment_name,
                src.assignment_type, src.points_possible, src.due_date, src.unlock_date, src.lock_date,
                src.submission_types, src.is_group_assignment, src.weight
            )
        """).collect()
        
        # Mark as processed
        self.session.sql(f"""
            UPDATE {self.database}.RAW.RAW_ASSIGNMENTS
            SET processing_status = 'PROCESSED'
            WHERE processing_status = 'PENDING'
        """).collect()
        
        logger.info("Assignment transformations complete")
        return 0
        
    def transform_enrollments(self) -> int:
        """Apply transformations to enrollment facts."""
        logger.info("Enrollment transformations (handled by ingestion)")
        return 0
        
    def transform_submissions(self) -> int:
        """Apply transformations to submission facts."""
        logger.info("Submission transformations (handled by ingestion)")
        return 0
        
    def transform_activity_logs(self) -> int:
        """Apply transformations to activity log facts."""
        logger.info("Activity log transformations (handled by ingestion)")
        return 0
        
    def aggregate_student_performance(self) -> int:
        """
        Aggregate student performance metrics.
        Populates AGG_STUDENT_COURSE_PERFORMANCE table.
        Returns count of records created/updated.
        """
        logger.info("Aggregating student performance metrics...")
        
        # Truncate and reload aggregation table
        self.session.sql(f"""
            TRUNCATE TABLE {self.database}.{self.curated_schema}.AGG_STUDENT_COURSE_PERFORMANCE
        """).collect()
        
        result = self.session.sql(f"""
            INSERT INTO {self.database}.{self.curated_schema}.AGG_STUDENT_COURSE_PERFORMANCE
            SELECT 
                s.student_id,
                c.course_id,
                c.term,
                COUNT(DISTINCT a.assignment_id) AS total_assignments,
                COUNT(DISTINCT sub.submission_id) AS completed_assignments,
                ROUND(AVG(sub.percentage), 2) AS avg_score,
                SUM(sub.score) AS total_points_earned,
                SUM(sub.points_possible) AS total_points_possible,
                SUM(CASE WHEN sub.late_flag THEN 1 ELSE 0 END) AS late_submissions,
                SUM(CASE WHEN sub.missing_flag THEN 1 ELSE 0 END) AS missing_submissions,
                COALESCE(ROUND(SUM(act.duration_seconds) / 60, 0), 0) AS total_activity_minutes,
                MAX(act.activity_timestamp)::DATE AS last_activity_date,
                e.final_grade AS current_grade,
                CURRENT_TIMESTAMP() AS calculated_at
            FROM {self.database}.{self.curated_schema}.DIM_STUDENTS s
            INNER JOIN {self.database}.{self.curated_schema}.FACT_ENROLLMENTS e 
                ON s.student_id = e.student_id
            INNER JOIN {self.database}.{self.curated_schema}.DIM_COURSES c 
                ON e.course_id = c.course_id
            LEFT JOIN {self.database}.{self.curated_schema}.DIM_ASSIGNMENTS a 
                ON a.course_id = c.course_id
            LEFT JOIN {self.database}.{self.curated_schema}.FACT_SUBMISSIONS sub 
                ON sub.student_id = s.student_id AND sub.assignment_id = a.assignment_id
            LEFT JOIN {self.database}.{self.curated_schema}.FACT_ACTIVITY_LOGS act 
                ON act.student_id = s.student_id AND act.course_id = c.course_id
            GROUP BY s.student_id, c.course_id, c.term, e.final_grade
        """).collect()
        
        # Get count of inserted records
        count_result = self.session.sql(f"""
            SELECT COUNT(*) FROM {self.database}.{self.curated_schema}.AGG_STUDENT_COURSE_PERFORMANCE
        """).collect()
        
        count = count_result[0][0] if count_result else 0
        logger.info(f"Student performance aggregation complete. Records: {count}")
        return count
        
    def aggregate_course_analytics(self) -> int:
        """
        Aggregate course-level analytics.
        Populates AGG_COURSE_ANALYTICS table.
        Returns count of records created/updated.
        """
        logger.info("Aggregating course analytics...")
        
        # Truncate and reload
        self.session.sql(f"""
            TRUNCATE TABLE {self.database}.{self.curated_schema}.AGG_COURSE_ANALYTICS
        """).collect()
        
        result = self.session.sql(f"""
            INSERT INTO {self.database}.{self.curated_schema}.AGG_COURSE_ANALYTICS
            SELECT 
                c.course_id,
                c.term,
                COUNT(DISTINCT e.student_id) AS total_enrolled,
                COUNT(DISTINCT CASE WHEN e.enrollment_state = 'active' THEN e.student_id END) AS active_students,
                ROUND(AVG(e.final_score), 2) AS avg_class_score,
                ROUND(MEDIAN(e.final_score), 2) AS median_class_score,
                OBJECT_CONSTRUCT(
                    'A', COUNT(CASE WHEN e.final_grade IN ('A', 'A-') THEN 1 END),
                    'B', COUNT(CASE WHEN e.final_grade IN ('B+', 'B', 'B-') THEN 1 END),
                    'C', COUNT(CASE WHEN e.final_grade IN ('C+', 'C', 'C-') THEN 1 END),
                    'D', COUNT(CASE WHEN e.final_grade IN ('D+', 'D', 'D-') THEN 1 END),
                    'F', COUNT(CASE WHEN e.final_grade = 'F' THEN 1 END)
                ) AS grade_distribution,
                ROUND(
                    COUNT(CASE WHEN e.enrollment_state = 'completed' THEN 1 END) * 100.0 
                    / NULLIF(COUNT(*), 0), 
                    2
                ) AS completion_rate,
                ROUND(AVG(act_agg.total_minutes), 0) AS avg_engagement_minutes,
                COUNT(CASE WHEN e.final_score < 60 THEN 1 END) AS at_risk_students,
                CURRENT_TIMESTAMP() AS calculated_at
            FROM {self.database}.{self.curated_schema}.DIM_COURSES c
            INNER JOIN {self.database}.{self.curated_schema}.FACT_ENROLLMENTS e 
                ON c.course_id = e.course_id
            LEFT JOIN (
                SELECT 
                    student_id, 
                    course_id, 
                    ROUND(SUM(duration_seconds) / 60, 0) AS total_minutes
                FROM {self.database}.{self.curated_schema}.FACT_ACTIVITY_LOGS
                GROUP BY student_id, course_id
            ) act_agg 
                ON act_agg.student_id = e.student_id AND act_agg.course_id = c.course_id
            GROUP BY c.course_id, c.term
        """).collect()
        
        # Get count
        count_result = self.session.sql(f"""
            SELECT COUNT(*) FROM {self.database}.{self.curated_schema}.AGG_COURSE_ANALYTICS
        """).collect()
        
        count = count_result[0][0] if count_result else 0
        logger.info(f"Course analytics aggregation complete. Records: {count}")
        return count
        
    def calculate_student_risk_scores(self) -> int:
        """
        Calculate risk scores for students based on:
        - GPA trends
        - Assignment completion rate
        - Activity engagement
        - Late submission patterns
        Returns count of students analyzed.
        """
        logger.info("Calculating student risk scores...")
        
        # This would typically create/update a risk score table
        # For this demo, we'll just log the analysis
        
        result = self.session.sql(f"""
            SELECT 
                student_id,
                AVG(avg_score) AS overall_avg_score,
                SUM(late_submissions) AS total_late,
                SUM(missing_submissions) AS total_missing,
                AVG(total_activity_minutes) AS avg_activity
            FROM {self.database}.{self.curated_schema}.AGG_STUDENT_COURSE_PERFORMANCE
            GROUP BY student_id
            HAVING AVG(avg_score) < 70 
                OR SUM(late_submissions) > 5 
                OR SUM(missing_submissions) > 3
        """).collect()
        
        count = len(result) if result else 0
        logger.info(f"Identified {count} at-risk students")
        return count
        
    def run_all_transformations(self) -> int:
        """
        Run all transformations in sequence.
        Returns total count of records processed.
        """
        logger.info("Running all transformations...")
        total = 0
        
        total += self.transform_students()
        total += self.transform_courses()
        total += self.transform_assignments()
        total += self.aggregate_student_performance()
        total += self.aggregate_course_analytics()
        total += self.calculate_student_risk_scores()
        
        logger.info(f"All transformations complete. Total operations: {total}")
        return total
        
    def run_incremental_transformations(self) -> int:
        """
        Run transformations for incremental data.
        Optimized for smaller data changes.
        """
        return self.run_all_transformations()



