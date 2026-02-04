"""
Demo Canvas ETL Service - Main Application Entry Point
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
    logger.info("Starting Demo Canvas ETL Service...")
    logger.info(f"Database: {os.getenv('SNOWFLAKE_DATABASE', 'DEMO_CANVAS_DB')}")
    logger.info(f"Log Level: {os.getenv('LOG_LEVEL', 'INFO')}")
    yield
    # Shutdown
    logger.info("Shutting down Demo Canvas ETL Service...")


# Create FastAPI app
app = FastAPI(
    title="Demo Canvas ETL Service",
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


@app.get("/status_get", response_model=ETLStatusResponse)
async def get_status_http():
    """
    Get current ETL pipeline status (HTTP GET version).
    Returns information about running jobs and statistics.
    """
    return ETLStatusResponse(
        status="running" if job_state["running_jobs"] else "idle",
        last_run=job_state["last_run"],
        records_processed=job_state["records_processed"],
        errors=job_state["errors"],
        running_jobs=job_state["running_jobs"]
    )


# Old async endpoint removed - now using Snowflake service function format


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


# Old transform endpoint removed - now using Snowflake service function format below


@app.get("/metrics")
async def get_metrics():
    """Get service metrics for monitoring."""
    return {
        "total_records_processed": job_state["records_processed"],
        "total_errors": job_state["errors"],
        "active_jobs": len(job_state["running_jobs"]),
        "uptime": "healthy"
    }


# ============================================================================
# SNOWFLAKE SERVICE FUNCTION ENDPOINTS
# These handle Snowflake's specific request/response format for service functions
# Format: {"data": [[row_index, arg1, arg2, ...], ...]}
# ============================================================================

class SnowflakeRequest(BaseModel):
    """Snowflake service function request format."""
    data: list

@app.post("/run_etl")
async def run_etl_snowflake(request: SnowflakeRequest):
    """
    Handle Snowflake service function calls for ETL.
    Snowflake sends: {"data": [[0, "JOB_TYPE"]]}
    We return: {"data": [[0, "result message"]]}
    """
    results = []
    
    for row in request.data:
        row_index = row[0]
        job_type = row[1] if len(row) > 1 else "FULL_REFRESH"
        
        logger.info(f"Snowflake service function called with job_type: {job_type}")
        
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
                    results.append([row_index, f"Unknown job type: {job_type}"])
                    continue
                
                job_state["records_processed"] += records
                job_state["last_run"] = datetime.utcnow().isoformat()
                
                results.append([row_index, f"ETL {job_type} completed. Records processed: {records}"])
                logger.info(f"ETL {job_type} completed. Records: {records}")
                
        except Exception as e:
            logger.error(f"ETL job failed: {e}")
            job_state["errors"] += 1
            results.append([row_index, f"Error: {str(e)}"])
    
    return {"data": results}


@app.post("/status")
async def get_status_snowflake(request: SnowflakeRequest):
    """
    Handle Snowflake service function calls for status.
    Returns current ETL status in Snowflake format.
    """
    import json
    
    results = []
    for row in request.data:
        row_index = row[0]
        status = {
            "status": "running" if job_state["running_jobs"] else "idle",
            "last_run": job_state["last_run"],
            "records_processed": job_state["records_processed"],
            "errors": job_state["errors"],
            "running_jobs": len(job_state["running_jobs"])
        }
        results.append([row_index, json.dumps(status)])
    
    return {"data": results}


@app.post("/transform")
async def transform_snowflake(request: SnowflakeRequest):
    """
    Handle Snowflake service function calls for transformations.
    Snowflake sends: {"data": [[0, "transformation_name"]]}
    """
    results = []
    
    for row in request.data:
        row_index = row[0]
        transformation_name = row[1] if len(row) > 1 else "student_dimension"
        
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
                    results.append([row_index, f"Transformation {transformation_name} completed. Records: {records}"])
                else:
                    results.append([row_index, f"Unknown transformation: {transformation_name}"])
                    
        except Exception as e:
            logger.error(f"Transformation failed: {e}")
            results.append([row_index, f"Error: {str(e)}"])
    
    return {"data": results}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8080)



