import io
import json
import logging
import os
import re
import time
import uuid
from datetime import datetime, timezone

from databricks.sdk import WorkspaceClient
from dotenv import load_dotenv

from startup_jobs import StartupJobsClient
from config import TARGET_ROLES, MAX_PAGES_PER_ROLE


load_dotenv()


# ---------------------------------------------------------
# Configuration
# ---------------------------------------------------------

# Unity Catalog Volume
BRONZE_VOLUME = (
    "/Volumes/workspace/jobpulse_bronze/raw_startup_jobs"
)

# Retry configuration
MAX_RETRIES = 3
INITIAL_RETRY_DELAY = 2

# Env vars this module depends on directly. If StartupJobsClient/config.py
# require others (e.g. an API key), add them here too.
REQUIRED_ENV_VARS = ["DATABRICKS_HOST", "DATABRICKS_TOKEN"]


# ---------------------------------------------------------
# Logging
# ---------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------
# Utility Functions
# ---------------------------------------------------------

def generate_run_id():
    """Generate a unique ID for each ingestion run."""
    return str(uuid.uuid4())


def sanitize_role(role):
    """Convert a role into a safe filename component."""

    safe_role = re.sub(
        r"[^a-zA-Z0-9_]+",
        "_",
        role
    )

    return safe_role.strip("_").lower()


def require_env_vars():
    """Fail fast with a clear message if required env vars are missing,
    instead of letting WorkspaceClient(...) raise a cryptic SDK error."""

    missing = [
        name for name in REQUIRED_ENV_VARS
        if not os.getenv(name)
    ]

    if missing:
        raise EnvironmentError(
            "Missing required environment variable(s): "
            f"{', '.join(missing)}. Check your .env file locally, "
            "or the container/Airflow environment."
        )


# ---------------------------------------------------------
# Bronze Storage
# ---------------------------------------------------------

def save_bronze(
    data,
    role,
    page_number,
    workspace_client
):
    """
    Save the raw API response to the Bronze
    Unity Catalog Volume.

    The API response structure is preserved exactly
    as returned by the source.
    """

    ingestion_time = datetime.now(timezone.utc)

    date_path = ingestion_time.strftime("%Y/%m/%d")

    safe_role = sanitize_role(role)

    filename = (
        f"jobs_"
        f"{ingestion_time.strftime('%Y%m%d_%H%M%S_%f')}"
        f"_{safe_role}"
        f"_page_{page_number}.json"
    )

    remote_path = (
        f"{BRONZE_VOLUME}/"
        f"{date_path}/"
        f"{filename}"
    )

    # Preserve the API response structure exactly.
    json_data = json.dumps(
        data,
        ensure_ascii=False,
        indent=4
    )

    workspace_client.files.upload(
        remote_path,
        io.BytesIO(json_data.encode("utf-8")),
        overwrite=False
    )

    logger.info(
        "Saved Bronze data | role=%s | page=%s | path=%s",
        role,
        page_number,
        remote_path
    )

    return remote_path


def save_bronze_with_retry(
    data,
    role,
    page_number,
    workspace_client
):
    """
    Save to Bronze with the same retry treatment as the API fetch,
    since an upload can fail transiently (network blip, throttling)
    just as easily as a request can.
    """

    for attempt in range(1, MAX_RETRIES + 1):

        try:

            return save_bronze(
                data=data,
                role=role,
                page_number=page_number,
                workspace_client=workspace_client
            )

        except Exception as exc:

            logger.warning(
                "Bronze upload failed | "
                "role=%s | page=%s | attempt=%s/%s | error=%s",
                role,
                page_number,
                attempt,
                MAX_RETRIES,
                exc
            )

            if attempt == MAX_RETRIES:
                raise

            delay = INITIAL_RETRY_DELAY * (
                2 ** (attempt - 1)
            )

            logger.info(
                "Retrying Bronze upload in %s seconds...",
                delay
            )

            time.sleep(delay)


# ---------------------------------------------------------
# API Validation
# ---------------------------------------------------------

def validate_api_response(data):
    """
    Perform basic validation without modifying
    the API response structure.
    """

    if not isinstance(data, dict):
        raise ValueError(
            "API response is not a dictionary."
        )

    if "data" not in data:
        raise ValueError(
            "API response is missing the 'data' field."
        )

    if not isinstance(data["data"], list):
        raise ValueError(
            "API response 'data' field is not a list."
        )


# ---------------------------------------------------------
# API Request With Retry
# ---------------------------------------------------------

def fetch_jobs_with_retry(
    client,
    role,
    starting_after
):
    """
    Fetch jobs from the API with retry handling
    for transient failures.
    """

    for attempt in range(1, MAX_RETRIES + 1):

        try:

            data = client.search_jobs(
                role=role,
                starting_after=starting_after
            )

            validate_api_response(data)

            return data

        except Exception as exc:

            logger.warning(
                "API request failed | "
                "role=%s | attempt=%s/%s | error=%s",
                role,
                attempt,
                MAX_RETRIES,
                exc
            )

            if attempt == MAX_RETRIES:
                raise

            delay = INITIAL_RETRY_DELAY * (
                2 ** (attempt - 1)
            )

            logger.info(
                "Retrying API request in %s seconds...",
                delay
            )

            time.sleep(delay)


# ---------------------------------------------------------
# Main Ingestion
# ---------------------------------------------------------

def run_ingestion():

    require_env_vars()

    run_id = generate_run_id()

    logger.info("=" * 60)
    logger.info("STARTING JOBPULSE INGESTION")
    logger.info("Run ID: %s", run_id)
    logger.info("=" * 60)

    # Databricks connection
    workspace_client = WorkspaceClient(
        host=os.getenv("DATABRICKS_HOST"),
        token=os.getenv("DATABRICKS_TOKEN")
    )

    client = StartupJobsClient()

    total_records_returned = 0
    unique_jobs = {}

    successful_roles = []
    failed_roles = []

    # -----------------------------------------------------
    # Process target roles
    # -----------------------------------------------------

    for role in TARGET_ROLES:

        logger.info("=" * 60)
        logger.info("INGESTING ROLE: %s", role)
        logger.info("=" * 60)

        starting_after = None
        page_number = 1
        role_failed = False

        while page_number <= MAX_PAGES_PER_ROLE:

            logger.info(
                "Fetching role=%s | page=%s",
                role,
                page_number
            )

            # -------------------------------------------------
            # Fetch API data
            # -------------------------------------------------

            try:

                data = fetch_jobs_with_retry(
                    client=client,
                    role=role,
                    starting_after=starting_after
                )

            except Exception as exc:

                logger.error(
                    "Failed to fetch jobs | "
                    "role=%s | page=%s | error=%s",
                    role,
                    page_number,
                    exc
                )

                role_failed = True
                break

            # -------------------------------------------------
            # Extract jobs
            # -------------------------------------------------

            jobs = data.get("data", [])

            logger.info(
                "Jobs returned | role=%s | page=%s | count=%s",
                role,
                page_number,
                len(jobs)
            )

            # -------------------------------------------------
            # Track unique jobs for this run
            #
            # Cross-run deduplication is handled downstream
            # in the Silver layer.
            # -------------------------------------------------

            for job in jobs:

                job_id = job.get("id")

                if job_id:
                    unique_jobs[job_id] = job

            total_records_returned += len(jobs)

            # -------------------------------------------------
            # Save raw API response to Bronze (now with retries)
            # -------------------------------------------------

            try:

                save_bronze_with_retry(
                    data=data,
                    role=role,
                    page_number=page_number,
                    workspace_client=workspace_client
                )

            except Exception as exc:

                logger.error(
                    "Failed to save Bronze data | "
                    "role=%s | page=%s | error=%s",
                    role,
                    page_number,
                    exc
                )

                role_failed = True
                break

            # -------------------------------------------------
            # Pagination
            # -------------------------------------------------

            starting_after = data.get("next_cursor")

            has_more = data.get(
                "has_more",
                False
            )

            logger.info(
                "Pagination | role=%s | page=%s | "
                "has_more=%s | next_cursor=%s",
                role,
                page_number,
                has_more,
                starting_after
            )

            # Stop when there are no more pages
            if not has_more or not starting_after:
                break

            page_number += 1

        # -----------------------------------------------------
        # Record role status
        # -----------------------------------------------------

        if role_failed:

            failed_roles.append(role)

            logger.error(
                "Role ingestion FAILED: %s",
                role
            )

        else:

            successful_roles.append(role)

            logger.info(
                "Role ingestion completed successfully: %s",
                role
            )

    # ---------------------------------------------------------
    # Final Metrics
    # ---------------------------------------------------------

    duplicate_records = (
        total_records_returned
        - len(unique_jobs)
    )

    logger.info("=" * 60)
    logger.info("INGESTION COMPLETE")
    logger.info("=" * 60)

    logger.info(
        "Run ID: %s",
        run_id
    )

    logger.info(
        "Total records returned: %s",
        total_records_returned
    )

    logger.info(
        "Unique jobs found this run: %s",
        len(unique_jobs)
    )

    logger.info(
        "Duplicate records this run: %s",
        duplicate_records
    )

    logger.info(
        "Successful roles: %s",
        len(successful_roles)
    )

    logger.info(
        "Failed roles: %s",
        len(failed_roles)
    )

    if failed_roles:

        logger.error(
            "Failed roles: %s",
            ", ".join(failed_roles)
        )

    logger.info("=" * 60)

    # ---------------------------------------------------------
    # Structured summary, returned on success and also useful
    # for logging/inspection even in the failure path below.
    # ---------------------------------------------------------

    summary = {
        "run_id": run_id,
        "total_records": total_records_returned,
        "unique_jobs": len(unique_jobs),
        "duplicates": duplicate_records,
        "successful_roles": successful_roles,
        "failed_roles": failed_roles,
    }

    # ---------------------------------------------------------
    # Fail pipeline if any role failed
    # ---------------------------------------------------------

    if failed_roles:

        raise RuntimeError(
            "JobPulse ingestion completed with failures. "
            f"Failed roles: {', '.join(failed_roles)}"
        )

    return summary


# ---------------------------------------------------------
# Entry Point
# ---------------------------------------------------------

if __name__ == "__main__":
    run_ingestion()
