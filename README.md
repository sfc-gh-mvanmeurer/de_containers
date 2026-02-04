# Demo Canvas Data Engineering Pipeline

End-to-end Snowflake demo: Snowpark Container Services ETL, scheduled tasks, cost auditing, and CI/CD.

## Quick Start

1. **Run SQL scripts in order** (in Snowsight):
   - `sql/01_setup_infrastructure.sql` - Database, warehouses, roles
   - `sql/02_canvas_data_model.sql` - Tables and schemas
   - `sql/03_dummy_data_generator.sql` - Synthetic data procedures
   - `sql/04_scheduled_task.sql` - Task definitions
   - `sql/05_container_service.sql` - SPCS deployment
   - `sql/06_cost_auditing.sql` - Cost monitoring views

2. **For CI/CD**, add GitHub secrets: `SNOWFLAKE_ACCOUNT`, `SNOWFLAKE_USER`, `SNOWFLAKE_PASSWORD`, `SNOWFLAKE_ROLE`, `SNOWFLAKE_DATABASE`, `SNOWFLAKE_SCHEMA`

## Project Structure

```
├── container/          # Snowpark Container Services app
│   ├── app/            # Python ETL code (ingestion, transformations)
│   ├── Dockerfile
│   └── spec.yaml
├── sql/                # Snowflake setup scripts (run in order)
├── intelligence/       # Semantic views and AI agent setup
└── docs/               # Architecture diagrams
```

## What's Included

- **Data Model**: Canvas LMS star schema (students, courses, enrollments, submissions, activity)
- **ETL**: Python-based pipeline in SPCS with FastAPI endpoints
- **Orchestration**: Snowflake Tasks with stream-based CDC
- **Cost Monitoring**: Views for compute/storage cost attribution
- **CI/CD**: GitHub Actions → Snowpark Container Registry

## Prerequisites

- Snowflake account with ACCOUNTADMIN privileges
- Docker (for local development)
