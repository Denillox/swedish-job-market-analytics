import requests
import json
import os
import pandas as pd
from datetime import datetime

def extract_job(hit):
    return{
        'job_id': hit['id'],
        'job_title': hit['headline'],
        'employer': hit['employer']['name'],
        'publication_date': hit.get('publication_date'),
        'application_deadline': hit.get('application_deadline'),
        'municipality': hit.get('workplace_address', {}).get('municipality'),
        'region': hit.get('workplace_address', {}).get('region'),
        'category': hit.get('occupation', {}).get('label'),
        'type': hit.get('employment_type', {}).get('label'),
        'hours': hit.get('working_hours_type', {}).get('label'),
        'experience': hit['experience_required'],
        'number_of_vacancies': hit['number_of_vacancies'],
        'description': hit.get('description', {}).get('text'),
        'search_term': None
    }


all_jobs = []

base_url = "https://jobsearch.api.jobtechdev.se/search"
search_terms = [
    "data engineer",
    "data analyst",
    "ai engineer",
    "python developer",
    "machine learning",
]

for term in search_terms:
    offset = 0
    while True:
        '''
        Parameters:
        q -> search query (ex. data engineer)
        limit -> nr of results
        offset -> pagination (skip first n results)
        municipality -> filter by city code
        '''
        params = {
            "q": term,
            "limit": 100,
            "offset": offset
        }
        res = requests.get(base_url, params=params)

        if res.status_code != 200:
            print(f"API error for '{term}': {res.status_code}")
            break

        data = res.json()
        hits = data["hits"]

        if not hits:
            break

        for hit in hits:
            job = extract_job(hit)
            job['search_term'] = term
            all_jobs.append(job)
        
        offset += 100
        print(f"Got {len(all_jobs)} jobs so far..")

        if offset >= data['total']['value']:
            break

df = pd.DataFrame(all_jobs)
df = df.drop_duplicates(subset='job_id')
df['collected_date'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
print(f"Total unique jobs fetched: {len(df)}")

df.to_csv('data/raw_jobs.csv', index=False)
print("Saved to data/raw_jobs.csv")