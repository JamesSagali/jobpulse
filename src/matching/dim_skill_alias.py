import dlt

from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    StringType
)


@dlt.table(
    name="jobpulse_gold.dim_skill_alias",
    comment="Alternative skill names and phrases mapped to canonical skills",
    table_properties={
        "quality": "gold",
        "pipelines.autoOptimize.managed": "true"
    }
)
def dim_skill_alias():

    aliases = [

        # ---------------------------------------------------------
        # Analytics & Business Intelligence
        # ---------------------------------------------------------

        # SQL
        (1, "sql"),
        (1, "structured query language"),

        # Excel
        (2, "excel"),
        (2, "microsoft excel"),
        (2, "ms excel"),
        (2, "spreadsheet"),
        (2, "spreadsheets"),

        # Power BI
        (3, "power bi"),
        (3, "powerbi"),
        (3, "microsoft power bi"),

        # Tableau
        (4, "tableau"),

        # Looker
        (5, "looker"),
        (5, "google looker"),

        # DAX
        (6, "dax"),
        (6, "data analysis expressions"),

        # Power Query
        (7, "power query"),
        (7, "powerquery"),
        (7, "m language"),

        # Data Visualization
        (8, "data visualization"),
        (8, "data visualisation"),
        (8, "data viz"),

        # Data Analysis
        (9, "data analysis"),
        (9, "data analytics"),

        # Business Intelligence
        (10, "business intelligence"),
        (10, "business analytics"),

        # Data Modeling
        (11, "data modeling"),
        (11, "data modelling"),
        (11, "data model"),
        (11, "data models"),

        # Data Cleaning
        (12, "data cleaning"),
        (12, "data cleansing"),
        (12, "data preparation"),
        (12, "data preprocessing"),

        # Statistics
        (13, "statistics"),
        (13, "statistical analysis"),
        (13, "statistical modeling"),
        (13, "statistical modelling"),

        # ---------------------------------------------------------
        # Programming & Development
        # ---------------------------------------------------------

        # Python
        (14, "python"),
        (14, "python 3"),
        (14, "python3"),
        (14, "python programming"),

        # R
        (15, "r programming"),
        (15, "r language"),
        (15, "r programming language"),

        # Pandas
        (16, "pandas"),
        (16, "python pandas"),

        # NumPy
        (17, "numpy"),
        (17, "num py"),

        # Jupyter
        (18, "jupyter"),
        (18, "jupyter notebook"),
        (18, "jupyter notebooks"),
        (18, "jupyterlab"),

        # Git
        (19, "git"),
        (19, "git version control"),

        # GitHub
        (20, "github"),
        (20, "github repositories"),
        (20, "github actions"),

        # ---------------------------------------------------------
        # Data Engineering
        # ---------------------------------------------------------

        # Databricks
        (21, "databricks"),
        (21, "databricks platform"),

        # Apache Spark
        (22, "spark"),
        (22, "apache spark"),
        (22, "spark sql"),

        # PySpark
        (23, "pyspark"),
        (23, "py spark"),
        (23, "python spark"),

        # Airflow
        (24, "airflow"),
        (24, "apache airflow"),
        (24, "airflow dags"),

        # Kafka
        (25, "kafka"),
        (25, "apache kafka"),
        (25, "kafka streams"),

        # dbt
        (26, "dbt"),
        (26, "data build tool"),
        (26, "dbt core"),

        # ETL
        (27, "etl"),
        (27, "extract transform load"),
        (27, "extract transform and load"),

        # ELT
        (28, "elt"),
        (28, "extract load transform"),
        (28, "extract load and transform"),

        # Data Pipelines
        (29, "data pipelines"),
        (29, "data pipeline"),
        (29, "pipeline development"),
        (29, "data pipeline development"),

        # Data Warehousing
        (30, "data warehousing"),
        (30, "data warehouse"),
        (30, "data warehouse design"),

        # Data Lake
        (31, "data lake"),
        (31, "data lakes"),

        # Lakehouse
        (32, "lakehouse"),
        (32, "data lakehouse"),
        (32, "lake house"),

        # Delta Lake
        (33, "delta lake"),
        (33, "delta tables"),

        # Docker
        (34, "docker"),
        (34, "docker containers"),
        (34, "containerization"),
        (34, "containerisation"),

        # ---------------------------------------------------------
        # Databases & Data Platforms
        # ---------------------------------------------------------

        # MySQL
        (35, "mysql"),
        (35, "my sql"),

        # PostgreSQL
        (36, "postgresql"),
        (36, "postgres"),
        (36, "postgre sql"),

        # SQL Server
        (37, "sql server"),
        (37, "microsoft sql server"),
        (37, "ms sql server"),
        (37, "mssql"),

        # Snowflake
        (38, "snowflake"),
        (38, "snowflake data cloud"),

        # BigQuery
        (39, "bigquery"),
        (39, "google bigquery"),
        (39, "big query"),

        # ---------------------------------------------------------
        # IT & Technical Support
        # ---------------------------------------------------------

        # Zendesk
        (40, "zendesk"),
        (40, "zendesk support"),

        # ServiceNow
        (41, "servicenow"),
        (41, "service now"),
        (41, "servicenow platform"),

        # Genesys Cloud
        (42, "genesys cloud"),
        (42, "genesys"),
        (42, "genesys cloud cx"),
        (42, "genesys cloud cx platform"),

        # Jira
        (43, "jira"),
        (43, "atlassian jira"),

        # Technical Support
        (44, "technical support"),
        (44, "tech support"),
        (44, "technical customer support"),
        (44, "technical assistance"),

        # Customer Support
        (45, "customer support"),
        (45, "customer care"),
        (45, "customer experience support"),

        # Customer Service
        (46, "customer service"),
        (46, "client service"),
        (46, "customer relations"),

        # Application Support
        (47, "application support"),
        (47, "application production support"),
        (47, "application support engineer"),
        (47, "software support"),

        # Production Support
        (48, "production support"),
        (48, "production support engineer"),
        (48, "production systems support"),

        # Troubleshooting
        (49, "troubleshooting"),
        (49, "technical troubleshooting"),
        (49, "system troubleshooting"),
        (49, "problem troubleshooting"),

        # Incident Management
        (50, "incident management"),
        (50, "incident handling"),
        (50, "incident response"),
        (50, "incident resolution"),

        # Root Cause Analysis
        (51, "root cause analysis"),
        (51, "rca"),
        (51, "root-cause analysis"),

        # SLA
        (52, "sla"),
        (52, "service level agreement"),
        (52, "service level agreements"),

        # Escalation Management
        (53, "escalation management"),
        (53, "escalation handling"),
        (53, "technical escalation"),
        (53, "support escalation"),

        # ITIL
        (54, "itil"),
        (54, "information technology infrastructure library"),

        # IT Operations
        (55, "it operations"),
        (55, "information technology operations"),
        (55, "it ops"),
        (55, "technology operations"),

        # IT Security
        (56, "it security"),
        (56, "information technology security"),
        (56, "information security"),
        (56, "information security management"),

        # IT Architecture
        (57, "it architecture"),
        (57, "information technology architecture"),
        (57, "technology architecture"),
        (57, "enterprise it architecture"),

        # IT Governance
        (58, "it governance"),
        (58, "information technology governance"),
        (58, "technology governance"),

        # IT Compliance
        (59, "it compliance"),
        (59, "information technology compliance"),
        (59, "technology compliance"),
        (59, "compliance management"),

        # IT Risk Management
        (60, "it risk management"),
        (60, "information technology risk management"),
        (60, "technology risk management"),
        (60, "it risk"),
        (60, "technology risk"),

        # Customer Support Tools
        (61, "customer support tools"),
        (61, "customer service tools"),
        (61, "customer service platforms"),

        # IT Help Desk
        (62, "it help desk"),
        (62, "help desk"),
        (62, "helpdesk"),
        (62, "it helpdesk"),
        (62, "technical help desk"),
        (62, "help desk support"),

        # IT Asset Management
        (63, "it asset management"),
        (63, "information technology asset management"),
        (63, "it asset management system"),
        (63, "it asset tracking"),
        (63, "technology asset management"),

        # IT Configuration Management
        (64, "it configuration management"),
        (64, "configuration management"),
        (64, "it configuration"),
        (64, "configuration management database"),
        (64, "cmdb"),

        # Support Tools
        (65, "support tools"),
        (65, "support tooling"),
        (65, "support platform"),
        (65, "support platforms"),
        (65, "technical support tools"),

        # IT Service Management
        (66, "it service management"),
        (66, "itsm"),
        (66, "information technology service management"),
        (66, "it service management process"),
        (66, "it service management framework"),

        # IT Service Desk
        (67, "it service desk"),
        (67, "service desk"),
        (67, "service desk support"),
        (67, "it support desk"),
        (67, "technical service desk"),

        # IT Service Level Agreement
        (68, "it service level agreement"),
        (68, "it service level agreements"),

        # IT Service Management Practices
        (69, "it service management practices"),
        (69, "itsm practices"),
        (69, "it service management practice"),
        (69, "service management practices")
    ]

    schema = StructType([
        StructField("skill_key", IntegerType(), False),
        StructField("alias", StringType(), False)
    ])

    return (
        spark.createDataFrame(aliases, schema)
        .dropDuplicates(["skill_key", "alias"])
    )