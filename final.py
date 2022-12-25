# Description: This file contains the code for scraping the data from the maersk website
# Python Version: 3.10.1
# author @shubhtoy


# Importing the required libraries
import requests
from requests.structures import CaseInsensitiveDict
import time
from threading import Thread
import json

# defining the url
url_base = "https://api.maersk.com/careers/vacancies?region=&category=&country=India&searchInput=&offset={offset}&limit=48&language=EN"
url_job = "https://api.maersk.com/careers/vacancy/wd/{job_id}"

# config
OUTFILE_NAME = "data"
external_url = []

# headers
headers = CaseInsensitiveDict()

headers["authority"] = "api.maersk.com"
headers["accept"] = "*/*"
headers["accept-language"] = "en-US,en;q=0.6"
headers["consumer-key"] = "07t5yq8mANPpVFiMirik4eLAA2K3mMQ4"
headers["origin"] = "https://www.maersk.com"
headers["referer"] = "https://www.maersk.com/"
headers["sec-fetch-dest"] = "empty"
headers["sec-fetch-mode"] = "cors"
headers["sec-fetch-site"] = "same-site"
headers["sec-gpc"] = "1"
headers[
    "user-agent"
] = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36"


# Functions
def get_jobs(offset, jobs, count=False):
    url_call = url_base.format(offset=offset)
    time.sleep(2)
    resp = requests.get(url_call, headers=headers)
    try:
        data = resp.json()
        _jobs = [i for i in data["results"]]
        jobs.extend(_jobs)
        if count:
            return jobs, data["resultCount"]
        return jobs
    except:
        print(url_call)
        print(resp.text)
        return jobs


def get_job(job_id):
    data = False
    try:
        url = url_job.format(job_id=job_id)
        resp = requests.get(url, headers=headers)
        data = resp.json()
    except Exception as e:
        data = get_job(job_id)
    return data


def get_total_pages():
    data, count = get_jobs(0, count=True, jobs=[])
    total_pages = int(count) // 48
    return total_pages


def get_all_jobs():
    total_pages = get_total_pages()
    total_pages += 1
    # print(total_pages)
    jobs = []
    threads = []

    # starting new threads
    for i in range(total_pages):
        thread = Thread(target=get_jobs, args=(i, jobs))
        threads.append(thread)
    for i in range(total_pages):
        threads[i].start()
    for i in range(total_pages):
        threads[i].join()

    return jobs


def middlewear(job, final_jobs):
    global external_url
    try:
        if job["Url"][-2] == "-":
            uri = job["Url"][-8:-2]
            if uri[0] == "_":
                uri = uri[1:]
            base = get_job(uri)

            job.update(base["Report_Entry"][0])
        else:
            uri = job["Url"][-6:]
            if uri[0] == "_":
                uri = uri[1:]
            base = get_job(uri)
            job.update(base["Report_Entry"][0])
        final_jobs.append(job)
    except Exception as e:
        # print(job["Url"])
        # print(e, job["Url"], uri)
        external_url.append(job["Url"])
    return


def get_all_job_details():

    jobs = get_all_jobs()
    print(f"Total Indian Jobs:{len(jobs)}")
    # print(len(jobs))
    job_details = []
    final_jobs = []
    threads = []
    for job in jobs:
        thread = Thread(target=middlewear, args=(job, final_jobs))
        threads.append(thread)
    for i in range(len(threads)):
        threads[i].start()
    for i in range(len(threads)):
        threads[i].join()

    return final_jobs


def to_json(data):
    global external_url

    with open("external_url.txt", "w") as f:
        for url in external_url:
            f.write(url + "\n")
    with open(f"{OUTFILE_NAME}.json", "w") as f:
        json.dump(data, f, indent=4)


def create_json():
    data = get_all_job_details()
    final_data = {}
    final_data["Company"] = "Maersk"
    final_data["Career Page"] = "https://www.maersk.com/careers"
    final_data["Jobs"] = data
    print("Total Jobs Scraped:", len(data))
    print("Total External Jobs:", len(external_url))
    to_json(final_data)


if __name__ == "__main__":
    # print(get_total_pages())
    # print(get_jobs(0))
    # print(get_job("R47614"))
    # print(get_all_jobs())
    create_json()
