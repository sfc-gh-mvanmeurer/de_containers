# FGCU Canvas Data Engineering Pipeline

> End-to-end demo showcasing Data Ingestion, Engineering, Cost Auditing, and CI/CD with Snowflake Snowpark Container Services

## 🎯 What This Demo Covers

| Component | Description |
|-----------|-------------|
| **Data Ingestion** | Python-based ETL running in Snowpark Container Services |
| **Data Engineering** | Transformation pipeline for Canvas LMS student data |
| **Dummy Data Task** | Scheduled task generating synthetic Canvas data |
| **Cost Auditing** | Compute cost monitoring and attribution |
| **CI/CD** | GitHub Actions → Snowpark Container Registry |

## 📁 Project Structure

```
fgcu_de_containers/
├── container/
│   ├── Dockerfile                 # Container image definition
│   ├── requirements.txt           # Python dependencies
│   ├── app/
│   │   ├── main.py               # Entry point
│   │   ├── ingestion.py          # Data ingestion logic
│   │   ├── transformations.py    # Data engineering transformations
│   │   └── utils.py              # Helper utilities
│   └── spec.yaml                 # Container service spec
├── sql/
│   ├── 01_setup_infrastructure.sql    # Database, schema, warehouse setup
│   ├── 02_canvas_data_model.sql       # Canvas data tables
│   ├── 03_dummy_data_generator.sql    # Stored proc for synthetic data
│   ├── 04_scheduled_task.sql          # Task definitions
│   ├── 05_container_service.sql       # SPCS setup
│   └── 06_cost_auditing.sql           # Cost monitoring views
├── .github/
│   └── workflows/
│       └── deploy.yml            # CI/CD pipeline
└── README.md
```

## 🚀 Quick Start

### Prerequisites

- Snowflake account with ACCOUNTADMIN or appropriate privileges
- GitHub account with repository access
- Docker (for local development)

### Step 1: Clone and Configure

```bash
git clone https://github.com/sfc-gh-mvanmeurer/fgcu_de_containers.git
cd fgcu_de_containers
```

### Step 2: Set Up Snowflake Infrastructure

Run the SQL scripts in order in Snowsight:

```sql
-- Execute in Snowsight SQL Worksheet
-- 1. Setup infrastructure
-- 2. Create Canvas data model
-- 3. Deploy dummy data generator
-- 4. Create scheduled task
-- 5. Deploy container service
-- 6. Setup cost auditing
```

### Step 3: Configure GitHub Secrets

Add these secrets to your GitHub repository:

| Secret | Description |
|--------|-------------|
| `SNOWFLAKE_ACCOUNT` | Your Snowflake account identifier |
| `SNOWFLAKE_USER` | Service account username |
| `SNOWFLAKE_PASSWORD` | Service account password |
| `SNOWFLAKE_ROLE` | Role with container registry access |
| `SNOWFLAKE_DATABASE` | Target database |
| `SNOWFLAKE_SCHEMA` | Target schema |

### Step 4: Trigger CI/CD

Push to `main` branch to trigger the deployment pipeline.

## 📊 Canvas Data Model

This demo models typical Canvas LMS data including:

- **Students** - Enrollment and demographic information
- **Courses** - Course catalog and sections
- **Assignments** - Assignment definitions and configurations
- **Submissions** - Student assignment submissions
- **Grades** - Grade records and GPA calculations
- **Enrollments** - Course enrollment mappings
- **Activity Logs** - Student engagement tracking

## 💰 Cost Auditing

The demo includes views for monitoring:

- Container compute usage
- Task execution costs
- Storage consumption
- Cost attribution by workflow component

## 🔄 CI/CD Pipeline

The GitHub Actions workflow:

1. **Build** - Creates Docker image
2. **Push** - Uploads to Snowpark Container Registry
3. **Deploy** - Updates container service specification
4. **Verify** - Confirms successful deployment

## 📝 License

Internal demo - Snowflake Solutions Engineering



