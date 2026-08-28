from startup_jobs import StartupJobsClient
import json


client = StartupJobsClient()

data = client.search_jobs()

print(f"Total jobs: {data.get('total_count')}")
print(f"Has more: {data.get('has_more')}")
print(f"Number returned: {len(data.get('data', []))}")

if data.get("data"):
    print("\nFIRST JOB:\n")

    print(
        json.dumps(
            data["data"][0],
            indent=4,
            ensure_ascii=False
        )
    )