"""
Demo Canvas ETL - Transformation Tests
=======================================
Unit tests for data transformation logic.
"""

import pytest
from unittest.mock import Mock, MagicMock
import json


class TestTransformationEngine:
    """Tests for TransformationEngine class."""
    
    def test_transform_students_no_updates(self):
        """Test student transformation when no records need updating."""
        # Mock session
        mock_session = Mock()
        mock_session.get_current_database.return_value = "DEMO_CANVAS_DB"
        mock_session.sql.return_value.collect.return_value = [(0,)]
        
        from app.transformations import TransformationEngine
        engine = TransformationEngine(mock_session)
        
        result = engine.transform_students()
        
        assert result == 0
        mock_session.sql.assert_called()
        
    def test_aggregate_student_performance_empty(self):
        """Test student performance aggregation with empty data."""
        mock_session = Mock()
        mock_session.get_current_database.return_value = "DEMO_CANVAS_DB"
        mock_session.sql.return_value.collect.return_value = [(0,)]
        
        from app.transformations import TransformationEngine
        engine = TransformationEngine(mock_session)
        
        result = engine.aggregate_student_performance()
        
        # Should return 0 for empty aggregation
        assert isinstance(result, int)


class TestDataIngestionPipeline:
    """Tests for DataIngestionPipeline class."""
    
    def test_get_pending_count_zero(self):
        """Test pending count when no records."""
        mock_session = Mock()
        mock_session.get_current_database.return_value = "DEMO_CANVAS_DB"
        mock_session.sql.return_value.collect.return_value = [{"CNT": 0}]
        
        from app.ingestion import DataIngestionPipeline
        pipeline = DataIngestionPipeline(mock_session)
        
        count = pipeline._get_pending_count("RAW_STUDENTS")
        
        assert count == 0
        
    def test_process_students_no_pending(self):
        """Test student processing with no pending records."""
        mock_session = Mock()
        mock_session.get_current_database.return_value = "DEMO_CANVAS_DB"
        mock_session.sql.return_value.collect.return_value = [{"CNT": 0}]
        
        from app.ingestion import DataIngestionPipeline
        pipeline = DataIngestionPipeline(mock_session)
        
        result = pipeline.process_students()
        
        assert result == 0


class TestUtilities:
    """Tests for utility functions."""
    
    def test_get_env_config_defaults(self):
        """Test environment config with default values."""
        import os
        
        # Clear any existing env vars
        for key in ["SNOWFLAKE_DATABASE", "SNOWFLAKE_SCHEMA_RAW"]:
            os.environ.pop(key, None)
        
        from app.utils import get_env_config
        config = get_env_config()
        
        assert config["database"] == "DEMO_CANVAS_DB"
        assert config["schema_raw"] == "RAW"
        assert config["schema_curated"] == "CURATED"
        
    def test_timer_context_manager(self):
        """Test Timer context manager."""
        import time
        from app.utils import Timer
        
        with Timer("Test Operation") as timer:
            time.sleep(0.1)
        
        assert timer.duration >= 0.1
        assert timer.duration < 0.5  # Should be quick


class TestFastAPIEndpoints:
    """Tests for FastAPI endpoints."""
    
    def test_health_endpoint_structure(self):
        """Test health endpoint response structure."""
        from app.main import HealthResponse
        
        response = HealthResponse(
            status="healthy",
            timestamp="2024-01-01T00:00:00",
            version="1.0.0",
            snowflake_connected=False
        )
        
        assert response.status == "healthy"
        assert response.version == "1.0.0"
        assert response.snowflake_connected is False
        
    def test_etl_status_response_structure(self):
        """Test ETL status response structure."""
        from app.main import ETLStatusResponse
        
        response = ETLStatusResponse(
            status="idle",
            last_run=None,
            records_processed=0,
            errors=0,
            running_jobs=[]
        )
        
        assert response.status == "idle"
        assert response.records_processed == 0
        assert len(response.running_jobs) == 0


# Integration tests (require Snowflake connection)
@pytest.mark.integration
class TestIntegration:
    """Integration tests requiring Snowflake connection."""
    
    @pytest.fixture
    def snowflake_session(self):
        """Create a Snowflake session for testing."""
        from app.utils import SnowflakeConnection
        
        try:
            with SnowflakeConnection() as session:
                yield session
        except Exception:
            pytest.skip("Snowflake connection not available")
    
    def test_snowflake_connection(self, snowflake_session):
        """Test Snowflake connection works."""
        result = snowflake_session.sql("SELECT 1 AS test").collect()
        assert result[0]["TEST"] == 1
        
    def test_database_exists(self, snowflake_session):
        """Test target database exists."""
        result = snowflake_session.sql(
            "SHOW DATABASES LIKE 'DEMO_CANVAS_DB'"
        ).collect()
        assert len(result) > 0



