"""
FGCU Canvas ETL Service - Main Application Entry Point
======================================================
FastAPI-based service for data ingestion and transformation operations.
"""

import os
import logging
from datetime import datetime
from contextlib import asynccontextmanager
from typing import Optional

from fastapi import FastAPI, HTTPException, BackgroundTasks
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from app.ingestion import DataIngestionPipeline
from app.transformations import TransformationEngine
from app.utils import SnowflakeConnection, setup_logging

# Configure logging
setup_logging()
logger = logging.getLogger(__name__)


class ETLJobRequest(BaseModel):
    """Request model for ETL job execution."""
    job_type: str = Field(..., description="Type of ETL job: FULL_REFRESH, INCREMENTAL, STUDENTS, COURSES, etc.")
    parameters: Optional[dict] = Field(default={}, description="Additional job parameters")


class TransformationRequest(BaseModel):
    """Request model for transformation execution."""
    transformation_name: str = Field(..., description="Name of transformation to run")
    params: Optional[dict] = Field(default={}, description="Transformation parameters")


class HealthResponse(BaseModel):
    """Health check response model."""
    status: str
    timestamp: str
    version: str
    snowflake_connected: bool


class ETLStatusResponse(BaseModel):
    """ETL status response model."""
    status: str
    last_run: Optional[str]
    records_processed: int
    errors: int
    running_jobs: list


# Global state for tracking jobs
job_state = {
    "last_run": None,
    "records_processed": 0,
    "errors": 0,
    "running_jobs": []
}


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan manager for startup/shutdown."""
    # Startup
    logger.info("Starting FGCU Canvas ETL Service...")
    logger.info(f"Database: {os.getenv('SNOWFLAKE_DATABASE', 'FGCU_CANVAS_DEMO')}")
    logger.info(f"Log Level: {os.getenv('LOG_LEVEL', 'INFO')}")
    yield
    # Shutdown
    logger.info("Shutting down FGCU Canvas ETL Service...")


# Create FastAPI app
app = FastAPI(
    title="FGCU Canvas ETL Service",
    description="Data ingestion and transformation service for Canvas LMS data",
    version="1.0.0",
    lifespan=lifespan
)


@app.get("/health", response_model=HealthResponse)
async def health_check():
    """
    Health check endpoint for container orchestration.
    Returns service health status and Snowflake connectivity.
    """
    try:
        # Try Snowflake connection
        sf_connected = False
        try:
            with SnowflakeConnection() as session:
                session.sql("SELECT 1").collect()
                sf_connected = True
        except Exception as e:
            logger.warning(f"Snowflake connection check failed: {e}")
        
        return HealthResponse(
            status="healthy",
            timestamp=datetime.utcnow().isoformat(),
            version="1.0.0",
            snowflake_connected=sf_connected
        )
    except Exception as e:
        logger.error(f"Health check failed: {e}")
        raise HTTPException(status_code=503, detail=str(e))


@app.get("/status", response_model=ETLStatusResponse)
async def get_status():
    """
    Get current ETL pipeline status.
    Returns information about running jobs and statistics.
    """
    return ETLStatusResponse(
        status="running" if job_state["running_jobs"] else "idle",
        last_run=job_state["last_run"],
        records_processed=job_state["records_processed"],
        errors=job_state["errors"],
        running_jobs=job_state["running_jobs"]
    )


@app.post("/run_etl")
async def run_etl(request: ETLJobRequest, background_tasks: BackgroundTasks):
    """
    Execute ETL job.
    
    Supported job types:
    - FULL_REFRESH: Complete data refresh
    - INCREMENTAL: Process only new/changed data
    - STUDENTS: Process student data only
    - COURSES: Process course data only
    - ENROLLMENTS: Process enrollment data only
    - SUBMISSIONS: Process submission data only
    - ACTIVITY: Process activity log data only
    """
    job_id = f"job_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}"
    
    logger.info(f"Starting ETL job: {job_id}, type: {request.job_type}")
    
    # Add to running jobs
    job_state["running_jobs"].append({
        "job_id": job_id,
        "job_type": request.job_type,
        "started_at": datetime.utcnow().isoformat()
    })
    
    # Run in background
    background_tasks.add_task(execute_etl_job, job_id, request.job_type, request.parameters)
    
    return JSONResponse(
        status_code=202,
        content={
            "message": "ETL job started",
            "job_id": job_id,
            "job_type": request.job_type
        }
    )


async def execute_etl_job(job_id: str, job_type: str, parameters: dict):
    """Execute ETL job in background."""
    try:
        with SnowflakeConnection() as session:
            pipeline = DataIngestionPipeline(session)
            engine = TransformationEngine(session)
            
            records = 0
            
            if job_type == "FULL_REFRESH":
                records += pipeline.process_all_raw_data()
                records += engine.run_all_transformations()
            elif job_type == "INCREMENTAL":
                records += pipeline.process_incremental()
                records += engine.run_incremental_transformations()
            elif job_type == "STUDENTS":
                records += pipeline.process_students()
                records += engine.transform_students()
            elif job_type == "COURSES":
                records += pipeline.process_courses()
                records += engine.transform_courses()
            elif job_type == "ENROLLMENTS":
                records += pipeline.process_enrollments()
                records += engine.transform_enrollments()
            elif job_type == "SUBMISSIONS":
                records += pipeline.process_submissions()
                records += engine.transform_submissions()
            elif job_type == "ACTIVITY":
                records += pipeline.process_activity_logs()
                records += engine.transform_activity_logs()
            else:
                raise ValueError(f"Unknown job type: {job_type}")
            
            job_state["records_processed"] += records
            job_state["last_run"] = datetime.utcnow().isoformat()
            
            logger.info(f"ETL job {job_id} completed. Records processed: {records}")
            
    except Exception as e:
        logger.error(f"ETL job {job_id} failed: {e}")
        job_state["errors"] += 1
    finally:
        # Remove from running jobs
        job_state["running_jobs"] = [
            j for j in job_state["running_jobs"] if j["job_id"] != job_id
        ]


@app.post("/transform")
async def run_transformation(request: TransformationRequest, background_tasks: BackgroundTasks):
    """
    Execute a specific transformation.
    
    Supported transformations:
    - student_dimension: Update student dimension table
    - course_dimension: Update course dimension table
    - assignment_dimension: Update assignment dimension table
    - enrollment_fact: Process enrollment facts
    - submission_fact: Process submission facts
    - activity_fact: Process activity log facts
    - student_performance_agg: Update student performance aggregations
    - course_analytics_agg: Update course analytics aggregations
    """
    logger.info(f"Starting transformation: {request.transformation_name}")
    
    background_tasks.add_task(
        execute_transformation,
        request.transformation_name,
        request.params
    )
    
    return JSONResponse(
        status_code=202,
        content={
            "message": "Transformation started",
            "transformation": request.transformation_name
        }
    )


async def execute_transformation(transformation_name: str, params: dict):
    """Execute transformation in background."""
    try:
        with SnowflakeConnection() as session:
            engine = TransformationEngine(session)
            
            method_map = {
                "student_dimension": engine.transform_students,
                "course_dimension": engine.transform_courses,
                "assignment_dimension": engine.transform_assignments,
                "enrollment_fact": engine.transform_enrollments,
                "submission_fact": engine.transform_submissions,
                "activity_fact": engine.transform_activity_logs,
                "student_performance_agg": engine.aggregate_student_performance,
                "course_analytics_agg": engine.aggregate_course_analytics
            }
            
            if transformation_name in method_map:
                records = method_map[transformation_name]()
                job_state["records_processed"] += records
                logger.info(f"Transformation {transformation_name} completed. Records: {records}")
            else:
                raise ValueError(f"Unknown transformation: {transformation_name}")
                
    except Exception as e:
        logger.error(f"Transformation {transformation_name} failed: {e}")
        job_state["errors"] += 1


@app.get("/metrics")
async def get_metrics():
    """Get service metrics for monitoring."""
    return {
        "total_records_processed": job_state["records_processed"],
        "total_errors": job_state["errors"],
        "active_jobs": len(job_state["running_jobs"]),
        "uptime": "healthy"
    }


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8080)



