import os
import requests
from pathlib import Path
from dotenv import load_dotenv


BASE_DIR = Path(__file__).resolve().parents[2]

load_dotenv(BASE_DIR / ".env")


class StartupJobsClient:

    BASE_URL = "https://api.startup.jobs/v1/jobs"

    def __init__(self):

        self.api_key = os.getenv(
            "STARTUP_JOBS_API_KEY"
        )

        if not self.api_key:
            raise ValueError(
                "STARTUP_JOBS_API_KEY is not set"
            )

        self.headers = {
            "Authorization": f"Bearer {self.api_key}"
        }

    def search_jobs(
        self,
        role=None,
        country=None,
        workplace_type=None,
        employment_type=None,
        starting_after=None
    ):

        params = {
            "limit": 20
        }

        if role:
            params["role"] = role

        if country:
            params["country"] = country

        if workplace_type:
            params["workplace_type"] = workplace_type

        if employment_type:
            params["employment_type"] = employment_type

        if starting_after:
            params["starting_after"] = starting_after

        response = requests.get(
            self.BASE_URL,
            headers=self.headers,
            params=params,
            timeout=30
        )

        response.raise_for_status()

        return response.json()