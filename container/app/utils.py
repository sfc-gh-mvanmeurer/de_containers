"""
Demo Canvas ETL - Utility Functions
====================================
Helper utilities for Snowflake connectivity and logging.
"""

import os
import logging
import json
from typing import Optional
from contextlib import contextmanager

from snowflake.snowpark import Session


def setup_logging(level: str = None):
    """
    Configure structured logging for the application.
    
    Args:
        level: Log level (DEBUG, INFO, WARNING, ERROR). 
               Defaults to LOG_LEVEL env var or INFO.
    """
    log_level = level or os.getenv("LOG_LEVEL", "INFO")
    
    # Configure root logger
    logging.basicConfig(
        level=getattr(logging, log_level.upper()),
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    # Set specific loggers
    logging.getLogger("snowflake.connector").setLevel(logging.WARNING)
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("httpx").setLevel(logging.WARNING)
    
    logger = logging.getLogger(__name__)
    logger.info(f"Logging configured at {log_level} level")


class SnowflakeConnection:
    """
    Context manager for Snowflake Snowpark sessions.
    
    Automatically handles connection creation and cleanup.
    Uses environment variables or Snowpark Container Services
    auto-authentication when running inside SPCS.
    """
    
    def __init__(
        self,
        account: str = None,
        user: str = None,
        password: str = None,
        database: str = None,
        schema: str = None,
        warehouse: str = None,
        role: str = None
    ):
        """
        Initialize connection parameters.
        All parameters default to environment variables if not provided.
        """
        self.connection_params = {
            "account": account or os.getenv("SNOWFLAKE_ACCOUNT"),
            "user": user or os.getenv("SNOWFLAKE_USER"),
            "password": password or os.getenv("SNOWFLAKE_PASSWORD"),
            "database": database or os.getenv("SNOWFLAKE_DATABASE", "DEMO_CANVAS_DB"),
            "schema": schema or os.getenv("SNOWFLAKE_SCHEMA", "RAW"),
            "warehouse": warehouse or os.getenv("SNOWFLAKE_WAREHOUSE", "DEMO_TRANSFORM_WH"),
            "role": role or os.getenv("SNOWFLAKE_ROLE")
        }
        
        # Remove None values
        self.connection_params = {k: v for k, v in self.connection_params.items() if v}
        
        self.session: Optional[Session] = None
        self.logger = logging.getLogger(__name__)
        
    def __enter__(self) -> Session:
        """Create and return a Snowpark session."""
        try:
            # Check if running inside Snowpark Container Services
            if self._is_running_in_spcs():
                self.logger.info("Running in SPCS - using Snowflake login token")
                
                # Method 1: Try using the SPCS login token (most common approach)
                import httpx
                
                # Get login token from SPCS metadata service
                token_url = "http://localhost:8085/v1/token"
                
                try:
                    # Request a login token from the local metadata service
                    resp = httpx.get(token_url, timeout=5)
                    resp.raise_for_status()
                    token_data = resp.json()
                    
                    self.session = Session.builder.configs({
                        "host": token_data.get("host", os.getenv("SNOWFLAKE_HOST")),
                        "account": token_data.get("account", os.getenv("SNOWFLAKE_ACCOUNT")),
                        "authenticator": "oauth",
                        "token": token_data.get("token"),
                        "database": self.connection_params.get("database", "DEMO_CANVAS_DB"),
                        "schema": self.connection_params.get("schema", "RAW"),
                        "warehouse": self.connection_params.get("warehouse", "DEMO_TRANSFORM_WH")
                    }).create()
                    
                except httpx.RequestError:
                    # Fallback: Try reading from token file
                    self.logger.info("Metadata service unavailable, trying token file")
                    token_file = "/snowflake/session/token"
                    
                    if os.path.exists(token_file):
                        with open(token_file, 'r') as f:
                            token = f.read().strip()
                        
                        self.session = Session.builder.configs({
                            "host": os.getenv("SNOWFLAKE_HOST"),
                            "account": os.getenv("SNOWFLAKE_ACCOUNT"),
                            "authenticator": "oauth",
                            "token": token,
                            "database": self.connection_params.get("database", "DEMO_CANVAS_DB"),
                            "schema": self.connection_params.get("schema", "RAW"),
                            "warehouse": self.connection_params.get("warehouse", "DEMO_TRANSFORM_WH")
                        }).create()
                    else:
                        raise ValueError("No SPCS authentication method available")
                
            else:
                # Local/external execution - use provided credentials
                self.logger.info("Running externally - using provided credentials")
                self.session = Session.builder.configs(self.connection_params).create()
            
            self.logger.info(f"Connected to Snowflake: {self.session.get_current_database()}")
            return self.session
            
        except Exception as e:
            self.logger.error(f"Failed to connect to Snowflake: {e}")
            raise
            
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Close the Snowpark session."""
        if self.session:
            try:
                self.session.close()
                self.logger.info("Snowflake session closed")
            except Exception as e:
                self.logger.warning(f"Error closing session: {e}")
                
    def _is_running_in_spcs(self) -> bool:
        """Check if running inside Snowpark Container Services."""
        # SPCS indicators: token file exists, specific env vars, or metadata service available
        indicators = [
            os.path.exists("/snowflake/session/token"),
            os.getenv("SNOWFLAKE_HOST") is not None and "snowflakecomputing" in os.getenv("SNOWFLAKE_HOST", ""),
            os.path.exists("/snowflake")
        ]
        return any(indicators)


@contextmanager
def snowflake_session(**kwargs):
    """
    Convenience context manager for Snowflake sessions.
    
    Usage:
        with snowflake_session(database="MYDB") as session:
            session.sql("SELECT 1").collect()
    """
    conn = SnowflakeConnection(**kwargs)
    session = conn.__enter__()
    try:
        yield session
    finally:
        conn.__exit__(None, None, None)


def get_env_config() -> dict:
    """
    Get configuration from environment variables.
    Returns dict with all relevant config values.
    """
    return {
        "database": os.getenv("SNOWFLAKE_DATABASE", "DEMO_CANVAS_DB"),
        "schema_raw": os.getenv("SNOWFLAKE_SCHEMA_RAW", "RAW"),
        "schema_curated": os.getenv("SNOWFLAKE_SCHEMA_CURATED", "CURATED"),
        "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE", "DEMO_TRANSFORM_WH"),
        "log_level": os.getenv("LOG_LEVEL", "INFO"),
        "is_spcs": os.getenv("SNOWFLAKE_SERVICE_TOKEN") is not None
    }


def log_etl_run(
    session: Session,
    run_type: str,
    status: str,
    records_processed: int = 0,
    error_message: str = None,
    metadata: dict = None
):
    """
    Log ETL run details to audit table.
    
    Args:
        session: Active Snowpark session
        run_type: Type of ETL run (FULL_REFRESH, INCREMENTAL, etc.)
        status: Run status (STARTED, COMPLETED, FAILED)
        records_processed: Number of records processed
        error_message: Error message if failed
        metadata: Additional metadata as dict
    """
    logger = logging.getLogger(__name__)
    database = session.get_current_database()
    
    try:
        metadata_json = json.dumps(metadata) if metadata else "NULL"
        error_escaped = error_message.replace("'", "''") if error_message else None
        
        if status == "STARTED":
            session.sql(f"""
                INSERT INTO {database}.AUDIT.ETL_RUN_LOG 
                    (run_type, started_at, status)
                VALUES 
                    ('{run_type}', CURRENT_TIMESTAMP(), '{status}')
            """).collect()
        elif status in ("COMPLETED", "FAILED"):
            session.sql(f"""
                UPDATE {database}.AUDIT.ETL_RUN_LOG
                SET 
                    completed_at = CURRENT_TIMESTAMP(),
                    status = '{status}',
                    records_processed = {records_processed},
                    error_message = {f"'{error_escaped}'" if error_escaped else "NULL"},
                    metadata = PARSE_JSON('{metadata_json}')
                WHERE run_type = '{run_type}'
                    AND status = 'STARTED'
                    AND completed_at IS NULL
            """).collect()
            
        logger.info(f"ETL run logged: {run_type} - {status}")
        
    except Exception as e:
        logger.warning(f"Failed to log ETL run: {e}")


def validate_data_quality(session: Session, table_name: str, checks: list) -> dict:
    """
    Run data quality checks on a table.
    
    Args:
        session: Active Snowpark session
        table_name: Fully qualified table name
        checks: List of check definitions
        
    Returns:
        Dict with check results
    """
    logger = logging.getLogger(__name__)
    results = {}
    
    for check in checks:
        check_name = check.get("name", "unnamed")
        check_query = check.get("query")
        threshold = check.get("threshold", 0)
        
        try:
            result = session.sql(check_query).collect()
            value = result[0][0] if result else 0
            passed = value <= threshold
            
            results[check_name] = {
                "passed": passed,
                "value": value,
                "threshold": threshold
            }
            
            if not passed:
                logger.warning(f"DQ check failed: {check_name} - value {value} exceeds threshold {threshold}")
                
        except Exception as e:
            logger.error(f"DQ check error: {check_name} - {e}")
            results[check_name] = {
                "passed": False,
                "error": str(e)
            }
            
    return results


class Timer:
    """Simple timer for measuring execution duration."""
    
    def __init__(self, name: str = "Operation"):
        self.name = name
        self.start_time = None
        self.end_time = None
        self.logger = logging.getLogger(__name__)
        
    def __enter__(self):
        import time
        self.start_time = time.time()
        self.logger.info(f"{self.name} started")
        return self
        
    def __exit__(self, *args):
        import time
        self.end_time = time.time()
        duration = self.end_time - self.start_time
        self.logger.info(f"{self.name} completed in {duration:.2f} seconds")
        
    @property
    def duration(self) -> float:
        """Get duration in seconds."""
        if self.start_time and self.end_time:
            return self.end_time - self.start_time
        return 0.0



