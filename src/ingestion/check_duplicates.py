import json
from pathlib import Path
from collections import defaultdict


BASE_DIR = Path(__file__).resolve().parents[2]

BRONZE_DIR = (
    BASE_DIR
    / "data"
    / "raw"
    / "startup_jobs"
)


jobs_by_id = defaultdict(list)


for file in sorted(BRONZE_DIR.rglob("*.json")):

    with open(
        file,
        "r",
        encoding="utf-8"
    ) as f:

        data = json.load(f)

    for job in data.get("data", []):

        job_id = job.get("id")

        jobs_by_id[job_id].append(
            file.name
        )


total_jobs = sum(
    len(files)
    for files in jobs_by_id.values()
)

unique_jobs = len(jobs_by_id)

duplicates = {
    job_id: files
    for job_id, files in jobs_by_id.items()
    if len(files) > 1
}


print(f"Total jobs loaded: {total_jobs}")
print(f"Unique job IDs: {unique_jobs}")
print(f"Duplicate records: {total_jobs - unique_jobs}")


print("\nDuplicate details:\n")


for job_id, files in list(
    duplicates.items()
)[:10]:

    print(f"Job ID: {job_id}")

    for file in files:
        print(f"  → {file}")

    print()