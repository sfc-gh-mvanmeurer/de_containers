# Snowflake Intelligence Setup for Canvas Analytics

This folder contains SQL scripts to set up **Snowflake Intelligence** with **Semantic Views** and **Cortex Analyst** on top of your curated Canvas LMS data.

## Prerequisites

- ✅ Completed data engineering pipeline (CURATED layer populated)
- ✅ Snowflake account with Cortex AI features enabled
- ✅ Appropriate roles and privileges

## Script Execution Order

```bash
# Run in Snowsight in this order:
01_semantic_views.sql          # Create semantic views for business concepts
02_cortex_search_setup.sql     # Set up Cortex Search for unstructured data
03_intelligence_agent.sql      # Configure the Snowflake Intelligence agent
```

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                    SNOWFLAKE INTELLIGENCE                        │
│                                                                  │
│  ┌──────────────┐    ┌──────────────┐    ┌──────────────┐       │
│  │   Natural    │───▶│   Cortex     │───▶│   Semantic   │       │
│  │   Language   │    │   Analyst    │    │   Views      │       │
│  │   Query      │    │              │    │              │       │
│  └──────────────┘    └──────────────┘    └──────────────┘       │
│                                                 │                │
│                                                 ▼                │
│  ┌─────────────────────────────────────────────────────────┐    │
│  │                    CURATED LAYER                         │    │
│  │  ┌─────────┐  ┌─────────┐  ┌─────────┐  ┌─────────────┐ │    │
│  │  │   DIM   │  │   DIM   │  │  FACT   │  │     AGG     │ │    │
│  │  │STUDENTS │  │ COURSES │  │ENROLLS  │  │ PERFORMANCE │ │    │
│  │  └─────────┘  └─────────┘  └─────────┘  └─────────────┘ │    │
│  └─────────────────────────────────────────────────────────┘    │
└─────────────────────────────────────────────────────────────────┘
```

## Semantic Views Overview

### Student Analytics View
Business concepts for student performance analysis:
- **Dimensions**: Student Name, Major, Classification, Enrollment Status
- **Metrics**: GPA, Course Count, Assignment Completion Rate, Average Grade

### Course Analytics View  
Business concepts for course and instructor analysis:
- **Dimensions**: Course Name, Department, Term, Instructor
- **Metrics**: Enrollment Count, Average Grade, Submission Rate, Activity Score

### Enrollment Analytics View
Business concepts for enrollment patterns:
- **Dimensions**: Student, Course, Term, Status
- **Metrics**: Total Enrollments, Active Students, Completion Rate

## Sample Questions for the Agent

After setup, you can ask natural language questions like:

**Student Performance:**
- "What is the average GPA by major?"
- "Which students have the highest grades in Computer Science courses?"
- "Show me students at risk of failing (GPA below 2.0)"

**Course Analytics:**
- "Which courses have the highest enrollment this term?"
- "What is the average grade distribution for Biology 101?"
- "Show course completion rates by department"

**Enrollment Trends:**
- "How many students are enrolled by classification?"
- "What is the enrollment trend over the last 3 terms?"
- "Which courses have the highest dropout rates?"

## Customization

### Adding New Metrics
Edit `01_semantic_views.sql` to add business-specific metrics:

```sql
ALTER SEMANTIC VIEW canvas_student_analytics ADD
  METRIC at_risk_count AS 'COUNT_IF(gpa < 2.0)'
    DESCRIPTION 'Number of students with GPA below 2.0';
```

### Adding New Dimensions
```sql
ALTER SEMANTIC VIEW canvas_student_analytics ADD
  DIMENSION academic_standing AS 
    CASE WHEN gpa >= 3.5 THEN 'Dean''s List'
         WHEN gpa >= 2.0 THEN 'Good Standing'
         ELSE 'Academic Probation' END
    DESCRIPTION 'Academic standing based on GPA';
```

## Troubleshooting

### "Semantic view not found"
Ensure the semantic views were created successfully:
```sql
SHOW SEMANTIC VIEWS IN SCHEMA FGCU_CANVAS_DEMO.ANALYTICS;
```

### "Cortex Analyst not responding"
Check that your account has Cortex AI enabled:
```sql
SELECT SYSTEM$BEHAVIOR_CHANGE_BUNDLE_STATUS('2024_08');
```

### "Permission denied"
Grant necessary privileges:
```sql
GRANT USAGE ON SCHEMA FGCU_CANVAS_DEMO.ANALYTICS TO ROLE <your_role>;
GRANT REFERENCES ON ALL SEMANTIC VIEWS IN SCHEMA FGCU_CANVAS_DEMO.ANALYTICS TO ROLE <your_role>;
```

## References

- [Semantic Views Overview](https://docs.snowflake.com/en/user-guide/views-semantic/overview)
- [Snowflake Intelligence](https://docs.snowflake.com/en/user-guide/snowflake-cortex/snowflake-intelligence)
- [Cortex Analyst](https://docs.snowflake.com/en/user-guide/snowflake-cortex/cortex-analyst)

