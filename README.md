# Clinical SDTM Data Engineering Pipeline

## Features

- PySpark-based transformation of clinical DM domain
- CDISC SDTM-compliant mapping
- Data quality validation with Deequ
- Airflow DAG orchestration
- CI/CD via GitHub Actions

## Folder Structure

```
glue_jobs/       - PySpark jobs for ETL
dags/            - Airflow DAGs
validations/     - Deequ validation scripts
mappings/        - SDTM mapping JSONs
utils/           - Helper functions (e.g. S3 uploader)
.github/         - CI/CD workflows
```
