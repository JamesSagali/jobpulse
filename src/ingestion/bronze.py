import json
from pathlib import Path
from datetime import datetime, timezone

from startup_jobs import StartupJobsClient


BASE_DIR = Path(__file__).resolve().parents[2]

BRONZE_DIR = (
    BASE_DIR
    / "data"
    / "raw"
    / "startup_jobs"
)


def save_bronze(data, page_number):

    BRONZE_DIR.mkdir(
        parents=True,
        exist_ok=True
    )

    ingestion_time = datetime.now(timezone.utc)

    output_dir = (
        BRONZE_DIR
        / ingestion_time.strftime("%Y")
        / ingestion_time.strftime("%m")
        / ingestion_time.strftime("%d")
    )

    output_dir.mkdir(
        parents=True,
        exist_ok=True
    )

    filename = (
        f"jobs_"
        f"{ingestion_time.strftime('%Y%m%d_%H%M%S')}"
        f"_page_{page_number}.json"
    )

    output_path = output_dir / filename

    with open(
        output_path,
        "w",
        encoding="utf-8"
    ) as file:

        json.dump(
            data,
            file,
            ensure_ascii=False,
            indent=4
        )

    print(
        f"Saved page {page_number}: "
        f"{output_path}"
    )

    return output_path


def main():

    client = StartupJobsClient()

    starting_after = None
    page_number = 1

    total_jobs = 0

    MAX_PAGES = 3

    while page_number <= MAX_PAGES:

        print(
            f"\nFetching page {page_number}..."
        )

        data = client.search_jobs(
            starting_after=starting_after
        )

        jobs = data.get("data", [])

        total_jobs += len(jobs)

        save_bronze(
            data,
            page_number
        )

        print(
            f"Next cursor: {data.get('next_cursor')}"
        )

        print(
            f"Jobs collected: {total_jobs}"
        )

        has_more = data.get(
            "has_more",
            False
        )

        starting_after = data.get(
            "next_cursor"
        )

        if not has_more or not starting_after:
            break

        page_number += 1

    print("\nIngestion complete.")
    print(
        f"Total jobs collected: {total_jobs}"
    )


if __name__ == "__main__":
    main()