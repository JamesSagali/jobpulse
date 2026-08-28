from startup_jobs import StartupJobsClient


client = StartupJobsClient()


# PAGE 1
page_1 = client.search_jobs()

print("\nPAGE 1")
print("Jobs:", len(page_1["data"]))
print("Cursor:", page_1.get("next_cursor"))

cursor = page_1.get("next_cursor")


# PAGE 2
page_2 = client.search_jobs(
    starting_after=cursor
)

print("\nPAGE 2")
print("Jobs:", len(page_2["data"]))
print("Cursor:", page_2.get("next_cursor"))


# Compare IDs
ids_1 = {
    job["id"]
    for job in page_1["data"]
}

ids_2 = {
    job["id"]
    for job in page_2["data"]
}


print("\nCOMPARISON")

print(
    "Page 1 unique IDs:",
    len(ids_1)
)

print(
    "Page 2 unique IDs:",
    len(ids_2)
)

print(
    "Overlap:",
    len(ids_1 & ids_2)
)

print(
    "New jobs on page 2:",
    len(ids_2 - ids_1)
)