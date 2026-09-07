# JobPulse — Remote Job Market Analytics & Job Matching Platform

> An end-to-end data engineering and analytics project that ingests remote job listings, processes and models them in Databricks, matches jobs against a structured candidate skills database and target roles, and presents the results through an interactive Power BI dashboard.

---

## Table of Contents

- [Project Overview](#project-overview)
- [Objectives](#objectives)
- [Architecture](#architecture)
- [Technology Stack](#technology-stack)
- [Data Pipeline](#data-pipeline)
- [Project Structure](#project-structure)
- [Data Source](#data-source)
- [Bronze Layer](#bronze-layer)
- [Silver Layer](#silver-layer)
- [Gold Layer](#gold-layer)
- [Dimensional Model](#dimensional-model)
- [Role Modeling](#role-modeling)
- [Skill Database](#skill-database)
- [Job Skill Matching](#job-skill-matching)
- [Candidate Role Profiles](#candidate-role-profiles)
- [Job Matching Algorithm](#job-matching-algorithm)
- [Match Scoring](#match-scoring)
- [Power BI Dashboard](#power-bi-dashboard)
- [Airflow Orchestration](#airflow-orchestration)
- [Docker](#docker)
- [Configuration](#configuration)
- [Running the Project](#running-the-project)
- [Data Quality & Validation](#data-quality--validation)
- [Design Decisions](#design-decisions)
- [Known Limitations](#known-limitations)
- [Future Improvements](#future-improvements)

---

# Project Overview

**JobPulse** is an end-to-end data engineering and analytics platform for analyzing remote job opportunities and identifying jobs that best match a candidate's target roles and skills.

The project combines:

- API ingestion
- Python
- Apache Airflow
- Docker
- Databricks
- PySpark
- Delta Lake
- Medallion architecture
- Dimensional modeling
- SQL
- Role normalization
- Canonical skill modeling
- Job-to-skill matching
- Candidate-role matching
- Explainable scoring
- Power BI

The platform transforms raw job listings into an analytics-ready Gold layer and then applies a structured matching model to identify relevant job opportunities.

---

# Objectives

JobPulse addresses two related problems.

## 1. Remote Job Market Analytics

The platform provides insight into:

- Total remote jobs
- Companies hiring
- Job roles
- Employment types
- Salary availability
- Salary distributions
- Job posting trends

## 2. Candidate-Job Matching

The matching system identifies jobs based on:

- Candidate target roles
- Role-specific relevant skills
- Skills detected in job descriptions
- Role alignment
- Overall match score

The matching model is designed to be **transparent and explainable**.

---

# Architecture

```text
                         ┌─────────────────────┐
                         │   StartupJobs API   │
                         └──────────┬──────────┘
                                    │
                                    ▼
                         ┌─────────────────────┐
                         │ Python Ingestion    │
                         └──────────┬──────────┘
                                    │
                                    ▼
                         ┌─────────────────────┐
                         │ Airflow + Docker    │
                         └──────────┬──────────┘
                                    │
                                    ▼
                    ┌──────────────────────────────┐
                    │         DATABRICKS           │
                    │                              │
                    │    Bronze → Silver → Gold    │
                    └──────────────┬───────────────┘
                                   │
                 ┌─────────────────┴─────────────────┐
                 │                                   │
                 ▼                                   ▼
          Analytics Model                     Matching Model
                 │                                   │
                 └─────────────────┬─────────────────┘
                                   │
                                   ▼
                         ┌─────────────────────┐
                         │      Power BI       │
                         │                     │
                         │ Job Market Overview │
                         │ My Job Matches      │
                         │ Job Explorer        │
                         └─────────────────────┘
```

---

# Technology Stack

| Technology | Purpose |
|---|---|
| Python | API ingestion and processing |
| StartupJobs API | Job listing source |
| Apache Airflow | Pipeline orchestration |
| Docker | Containerization |
| Databricks | Lakehouse and data processing |
| PySpark | Data transformation |
| Delta Lake | Lakehouse storage |
| Unity Catalog Volumes | Raw data storage |
| SQL | Data validation and analysis |
| Power BI | Dashboard and visualization |
| Git/GitHub | Version control |

---

# Data Pipeline

The pipeline follows a medallion architecture:

```text
StartupJobs API
      │
      ▼
   BRONZE
Raw API responses
      │
      ▼
   SILVER
Cleaned and standardized jobs
      │
      ▼
    GOLD
Analytics + matching model
      │
      ▼
  POWER BI
```

---

# Project Structure

```text
jobpulse/
│
├── airflow/
│   └── dags/
│       └── jobpulse_pipeline.py
│
├── src/
│   ├── ingestion/
│   │   └── startup_jobs/
│   │       ├── __init__.py
│   │       └── ingest.py
│   │
│   └── ...
│
├── databricks/
│   ├── bronze/
│   ├── silver/
│   └── gold/
│
├── data/
│   ├── raw/
│   └── processed/
│
├── docker-compose.yml
├── requirements.txt
├── .env
├── .gitignore
└── README.md
```

Sensitive credentials and local data are excluded from version control.

---

# Data Source

JobPulse uses the **StartupJobs API** as its primary source of job listings.

The ingestion process queries the API using target job-role slugs such as:

```text
data-engineer
data-analyst
customer-support
analytics-engineer
support-specialist
support-analyst
technical-support
```

Retrieved job records include information such as:

- Job ID
- Job title
- Job URL
- Company
- Company website
- Employment type
- Job description
- Location
- Remote status
- Salary
- Published date
- Role titles
- Role slugs

---

# Bronze Layer

The Bronze layer stores raw API responses with minimal transformation.

Raw JSON files are stored in a Databricks Volume:

```text
/Volumes/workspace/jobpulse_bronze/raw_startup_jobs
```

Files follow a pattern similar to:

```text
jobs_{timestamp}_{role}_page_{page_number}.json
```

The Bronze layer provides:

- Raw-data preservation
- Traceability
- Replayability
- Separation between ingestion and transformation

---

# Silver Layer

The Silver layer transforms the raw API data into cleaned and standardized job records.

Transformations include:

- Schema normalization
- Data-type conversion
- Null handling
- Salary field processing
- Role normalization
- Remote-job identification
- Duplicate detection
- Timestamp handling

The original source job identifier is retained as:

```text
id
```

This identifier is preserved throughout the Gold and matching layers.

---

# Gold Layer

The Gold layer contains the business-ready analytical and matching model.

Core tables include:

```text
fact_remote_jobs

dim_company
dim_location
dim_employment_type
dim_date
dim_salary
dim_role

dim_role_alias

dim_skill
dim_skill_alias

bridge_job_role

candidate_role_match
job_skill_match
job_match_results
```

The Gold layer is designed specifically for analytical consumption and Power BI.

---

# Dimensional Model

## Fact Table

### `fact_remote_jobs`

The fact table represents remote job postings.

Important fields include:

```text
id
job_title
job_url
description
company_key
location_key
employment_type_key
date_key
salary_key
salary_min
salary_max
salary_mid
has_salary_info
is_remote
published_at
```

The source `id` is intentionally retained rather than renamed to `job_id`.

---

# Dimensions

## `dim_company`

Contains standardized company information.

```text
company_key
company_name
company_slug
company_website
```

## `dim_location`

Contains job-location attributes.

```text
location_key
location_full
location_short
country
country_code
```

## `dim_employment_type`

Contains employment classifications.

## `dim_date`

Provides calendar attributes for time-based analysis.

## `dim_salary`

Provides salary-band information used for salary analysis.

---

# Role Modeling

## `dim_role`

The role dimension contains normalized roles from the job source together with canonical candidate roles.

Fields:

```text
role_key
role_title
role_slug
role_category
```

Canonical candidate roles are:

```text
Data Analyst
Data Engineer
BI Analyst
Analytics Engineer
Technical Support Analyst
Customer Support Specialist
```

Canonical roles are explicitly included so that the matching system has a stable role vocabulary even when an exact role title does not occur in the source data.

---

# Role Aliases

## `dim_role_alias`

`dim_role_alias` maps variations of job titles to canonical candidate roles.

For example:

```text
Data Analyst
├── data analyst
├── reporting analyst
├── business data analyst
└── senior data analyst

Data Engineer
├── data engineer
├── etl engineer
├── data platform engineer
└── senior data engineer

BI Analyst
├── bi analyst
├── business intelligence analyst
├── bi developer
└── business intelligence specialist
```

Support roles are similarly normalized.

This enables jobs with different source titles to be associated with the candidate's target roles.

---

# Skill Database

## `dim_skill`

A key design decision in JobPulse is that **skills are modeled as a canonical database rather than simply being hardcoded inside the matching algorithm**.

The project currently contains **55 canonical skills**.

Examples include:

### Analytics

```text
SQL
Excel
Power Query
Data Visualization
Data Analysis
Data Modeling
Data Cleaning
Statistics
```

### Business Intelligence

```text
Power BI
Tableau
Looker
DAX
Business Intelligence
```

### Programming

```text
Python
R
Pandas
NumPy
Jupyter
```

### Data Engineering

```text
Databricks
Apache Spark
PySpark
Airflow
Kafka
dbt
ETL
ELT
Data Pipelines
Data Warehousing
Data Lake
Lakehouse
Delta Lake
Docker
```

### Databases and Data Platforms

```text
MySQL
PostgreSQL
SQL Server
Snowflake
BigQuery
```

### Support

```text
Zendesk
ServiceNow
Genesys Cloud
Jira
Technical Support
Customer Support
Customer Service
```

### IT Support

```text
Application Support
Production Support
Troubleshooting
Incident Management
Root Cause Analysis
SLA
Escalation Management
```

### Cloud

```text
AWS
Azure
```

---

# Skill Aliases

## `dim_skill_alias`

The canonical skill database is supported by a skill-alias table.

This separates:

```text
What a skill is called
```

from:

```text
How that skill may appear in a job description.
```

For example:

```text
Power BI
├── power bi
├── powerbi
└── microsoft power bi
```

maps to one canonical skill:

```text
Power BI
```

Similarly:

```text
PostgreSQL
├── postgresql
├── postgres
└── postgre sql
```

maps to:

```text
PostgreSQL
```

This approach allows aliases to be maintained independently of the matching logic.

Broad or ambiguous aliases are avoided where they could create false positives.

---

# Job Skill Matching

## `job_skill_match`

`job_skill_match` is the detailed **job-to-skill matching table**.

It connects individual jobs to the canonical skills stored in `dim_skill`.

The table contains:

```text
id
skill_key
skill_name
skill_category
skill_matched
```

Conceptually:

```text
                 dim_skill
                     │
                     │ skill_key
                     ▼
              job_skill_match
                     ▲
                     │ id
                     │
              fact_remote_jobs
```

Each record represents evidence for a specific:

```text
Job × Canonical Skill
```

For example:

```text
Job: 9793855

SQL          → TRUE
Python       → TRUE
Databricks   → TRUE
Airflow      → FALSE
Kafka        → FALSE
Docker       → TRUE
```

The important point is that the **55 skills are the canonical skill database**.

A job is not expected to match all 55 skills.

Instead, the candidate's target role determines which skills are relevant.

---

# Candidate Role Profiles

## `candidate_role_match`

`candidate_role_match` defines the candidate's target roles and the skills relevant to each role.

This table replaces the need for a separate role-skill dimension.

Its structure is:

```text
candidate_role
candidate_role_weight
skill_name
```

For example:

```text
Data Analyst
    SQL
    Excel
    Power BI
    Tableau
    Data Analysis
    Statistics
    Python
    ...

Data Engineer
    SQL
    Python
    Databricks
    Apache Spark
    PySpark
    Airflow
    Kafka
    dbt
    ...
```

The table therefore acts as the candidate's **role-specific skill profile**.

---

# Candidate Role Profiles

Current profiles contain:

| Candidate Role | Relevant Skills |
|---|---:|
| Data Analyst | 18 |
| Data Engineer | 27 |
| BI Analyst | 16 |
| Analytics Engineer | 17 |
| Technical Support Analyst | 17 |
| Customer Support Specialist | 14 |

Role priority is represented by `candidate_role_weight`.

Current weights:

| Candidate Role | Weight |
|---|---:|
| Data Analyst | 0.25 |
| Data Engineer | 0.25 |
| BI Analyst | 0.15 |
| Analytics Engineer | 0.15 |
| Technical Support Analyst | 0.10 |
| Customer Support Specialist | 0.10 |

The weight is primarily used as a **tie-breaker** when multiple candidate roles have comparable matches.

---

# Job Matching Algorithm

The matching process works through the relationship between the **skill database**, **skill aliases**, **job skill evidence**, and **candidate role profiles**.

```text
                  dim_skill
                     │
                     │ canonical skill
                     ▼
              dim_skill_alias
                     │
                     │ aliases
                     ▼
              job_skill_match
                     │
                     │ matched skills
                     ▼
           candidate_role_match
                     │
                     │ role-specific requirements
                     ▼
             job_match_results
```

The algorithm can be understood in four stages.

---

## Stage 1 — Build the Canonical Skill Database

The system maintains a controlled vocabulary of canonical skills in:

```text
dim_skill
```

This provides a consistent reference for skills across all jobs.

For example:

```text
Power BI
PostgreSQL
Apache Spark
Python
Databricks
```

are treated as canonical skills.

---

## Stage 2 — Normalize Skill Variations

`dim_skill_alias` stores alternative representations of canonical skills.

For example:

```text
"powerbi"
"microsoft power bi"
"power bi"
```

are associated with:

```text
Power BI
```

The aliases are normalized before being used to identify skills in job text.

This allows the system to match variations while maintaining a single canonical skill representation.

---

## Stage 3 — Create Job × Skill Evidence

The job title and description are evaluated against the canonical skill aliases.

The result is stored in:

```text
job_skill_match
```

For every relevant canonical skill, the table records whether that skill was found in the job.

This creates a reusable skill-evidence layer.

The matching engine therefore does **not** need to independently redefine the skill vocabulary every time it evaluates a candidate role.

The skills already exist as structured data.

---

## Stage 4 — Evaluate Skills Against the Candidate Role

`candidate_role_match` determines which skills matter for each target role.

For example:

```text
Data Analyst
    18 relevant skills

Data Engineer
    27 relevant skills
```

If a job is classified as a Data Analyst opportunity, its skill evidence is evaluated against the **18 Data Analyst skills**, not all 55 canonical skills.

Similarly, a Data Engineer job is evaluated against the **27 Data Engineer skills**.

---

# Skill Match Score

The role-specific skill score is calculated as:

```text
skills_matched
---------------------- × 100
relevant_skills
```

For example:

```text
Data Analyst role

10 matched skills
18 relevant skills

10 / 18 × 100
= 55.56%
```

This makes the score meaningful because the denominator represents the skills relevant to the candidate's target role.

---

# Role Matching

Jobs are first associated with candidate roles through:

```text
dim_role_alias
bridge_job_role
dim_role
```

For example:

```text
"