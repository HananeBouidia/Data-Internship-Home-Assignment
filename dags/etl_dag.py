from datetime import timedelta, datetime
from airflow.decorators import dag, task
from airflow.providers.sqlite.hooks.sqlite import SqliteHook
from airflow.providers.sqlite.operators.sqlite import SqliteOperator
from datetime import timedelta, datetime
from airflow.decorators import dag, task
from airflow.providers.sqlite.hooks.sqlite import SqliteHook
from airflow.providers.sqlite.operators.sqlite import SqliteOperator
import pandas as pd
import html
import re
import unicodedata
import json
import os



TABLES_CREATION_QUERY = ["""CREATE TABLE IF NOT EXISTS job (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    title VARCHAR(225),
    industry VARCHAR(225),
    description TEXT,
    employment_type VARCHAR(125),
    date_posted DATE
    );""",
    """
    CREATE TABLE IF NOT EXISTS company (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        job_id INTEGER,
        name VARCHAR(225),
        link TEXT,
        FOREIGN KEY (job_id) REFERENCES job(id)
    );
""",
"""
    CREATE TABLE IF NOT EXISTS education (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        job_id INTEGER,
        required_credential VARCHAR(225),
        FOREIGN KEY (job_id) REFERENCES job(id)
    );
""",
"""
    CREATE TABLE IF NOT EXISTS experience (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        job_id INTEGER,
        months_of_experience INTEGER,
        seniority_level VARCHAR(25),
        FOREIGN KEY (job_id) REFERENCES job(id)
    );
""",
"""
    CREATE TABLE IF NOT EXISTS salary (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        job_id INTEGER,
        currency VARCHAR(3),
        min_value NUMERIC,
        max_value NUMERIC,
        unit VARCHAR(12),
        FOREIGN KEY (job_id) REFERENCES job(id)
    );
""",
"""
    CREATE TABLE IF NOT EXISTS location (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        job_id INTEGER,
        country VARCHAR(60),
        locality VARCHAR(60),
        region VARCHAR(60),
        postal_code VARCHAR(25),
        street_address VARCHAR(225),
        latitude NUMERIC,
        longitude NUMERIC,
        FOREIGN KEY (job_id) REFERENCES job(id)
    );
    """
]
@task()
def extract():
    # Read the data from the CSV file into a DataFrame
    jobs_data = pd.read_csv(r"./jobs.csv")
    # Extract the context column data
    context_column = jobs_data['context']
    #save each item to staging/extracted as a text file
    for index, item in enumerate(context_column):
      with open(f"./staging/extracted/job_{index}.txt", "w") as file:
        file.write(str(item))

def clean_description(description):
    if description is None:
        return 'N/A'
    # Decode HTML entities
    cleaned_description = html.unescape(description)
    # Remove HTML tags using regular expression
    cleaned_description = re.sub('<.*?>', '', cleaned_description)
    # Convert Unicode characters to their readable form
    cleaned_description = unicodedata.normalize('NFKD', cleaned_description)

    # Add space between concatenated words
    cleaned_description = re.sub(r'([a-z])([A-Z])', r'\1 \2', cleaned_description)
    return cleaned_description


@task()
def transform():
    extracted_folder = './staging/extracted'
    transformed_folder = './staging/transformed'

    for filename in os.listdir(extracted_folder):
      with open(os.path.join(extracted_folder, filename), 'r') as file:
        try:
          data = json.load(file)
        except json.JSONDecodeError as e:
          continue
        except Exception as ex:
          continue
        # Handle 'experienceRequirements' key existence
        experience_req = data.get('experienceRequirements')
        months_of_experience = 'N/A'
        seniority_level = 'N/A'
        if experience_req and isinstance(experience_req, dict):
            months_of_experience = experience_req.get('monthsOfExperience', 'N/A')
            seniority_level = experience_req.get('Seniority', 'N/A')

        # Transform schema as needed
        transformed_data = {
            "job": {
                  "title": data.get('title', 'N/A'),
                  "industry": data.get('industry', 'N/A'),
                  "description": clean_description(data.get('description')),
                  "employment_type": data.get('employmentType', 'N/A'),
                  "date_posted": data.get('datePosted', 'N/A')
                  },
            "company": {
                      "name": data.get('hiringOrganization',{}).get('name','N/A'),
                      "link": data.get('hiringOrganization',{}).get('sameAs','N/A'),
                  },
            "education": {
                        "required_credential": data.get('educationRequirements',{}).get('credentialCategory','N/A'),
                  },
            "experience": {
                        "months_of_experience": months_of_experience,
                        "seniority_level": seniority_level,
                  },
            "salary": {
                "currency": data.get('estimatedSalary',{}).get('currency','N/A'),
                "min_value": data.get('estimatedSalary',{}).get('value',{}).get('minValue','N/A'),
                "max_value": data.get('estimatedSalary',{}).get('value',{}).get('maxValue','N/A'),
                "unit": data.get('estimatedSalary',{}).get('value',{}).get('unitText','N/A'),
            },
            "location": {
                "country": data.get('jobLocation',{}).get('address',{}).get('addressCountry','N/A'),
                "locality": data.get('jobLocation',{}).get('address',{}).get('addressLocality','N/A'),
                "region": data.get('jobLocation',{}).get('address',{}).get('addressRegion','N/A'),
                "postal_code": data.get('jobLocation',{}).get('address',{}).get('postalCode','N/A'),
                "street_address": data.get('jobLocation',{}).get('address',{}).get('streetAddress','N/A'),
                "latitude": data.get('jobLocation',{}).get('latitude','N/A'),
                "longitude": data.get('jobLocation',{}).get('longitude','N/A'),
            },
              }

        # Save the transformed data as JSON in the transformed folder
        transformed_filename = filename.replace('.txt', '_transformed.json')
        with open(os.path.join(transformed_folder, transformed_filename), 'w') as output_file:
          json.dump(transformed_data, output_file)


@task()
def load():
    """Load data to SQLite database."""
    sqlite_hook = SqliteHook(sqlite_conn_id='sqlite_etl')
    transformed_folder = './staging/transformed'
    
    
    sqlite_hook.run(TABLES_CREATION_QUERY)

    # Read each transformed file and save its contents to the database
    for filename in os.listdir(transformed_folder):
        with open(os.path.join(transformed_folder, filename), 'r') as file:
            data = json.load(file)
            # Insert into 'job' table
            job_query = """ INSERT INTO job (title, industry, description, employment_type, date_posted)
                    VALUES (?, ?, ?, ?, ?) """
            job_data = (
                    None,
                    data['job']['title'],
                    data['job']['industry'],
                    data['job']['description'],
                    data['job']['employment_type'],
                    data['job']['date_posted']
                )
            company_query = """ INSERT INTO company (name, link) VALUES (?, ?)"""
            company_data = (
                    None,
                    None,
                    data['company']['name'],
                    data['company']['link']
                )
            education_query =""" INSERT INTO education (required_credential) VALUES (?)"""
            education_data = (
                    None,
                    None,
                    data['education']['required_credential']
                )
            experience_query = """ INSERT INTO experience (months_of_experience,seniority_level) 
                                VALUES (?,?)"""
            experience_data = (
                    None,
                    None,
                    data['experience']['months_of_experience'],
                    data['experience']['seniority_level']
                )
            salary_query = """ INSERT INTO salary (currency, min_value, max_value, unit) VALUES (?, ?, ?, ?)"""
            salary_data = (
                    None,
                    None,
                    data['salary']['currency'],
                    data['salary']['min_value'],
                    data['salary']['max_value'],
                    data['salary']['unit']
                )
            location_query = """ INSERT INTO location (country, locality, region, postal_code, street_address,latitude,longitude)
                   VALUES (?, ?, ?, ?, ?, ?, ?) """
            location_data = (
                    None,
                    None,
                    data['location']['country'],
                    data['location']['locality'],
                    data['location']['region'],
                    data['location']['postal_code'],
                    data['location']['street_address'],
                    data['location']['latitude'],
                    data['location']['longitude']
                )
            
            sqlite_hook.insert_rows(table='job', rows=[job_data], commit_every=1)
            sqlite_hook.insert_rows(table='company', rows=[company_data], commit_every=1)
            sqlite_hook.insert_rows(table='education', rows=[education_data], commit_every=1)
            sqlite_hook.insert_rows(table='experience', rows=[experience_data], commit_every=1)
            sqlite_hook.insert_rows(table='salary', rows=[salary_data], commit_every=1)
            sqlite_hook.insert_rows(table='location', rows=[location_data], commit_every=1)


DAG_DEFAULT_ARGS = {
    "depends_on_past": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=15)
}

@dag(
    dag_id="etl_dag",
    description="ETL LinkedIn job posts",
    tags=["etl"],
    schedule="@daily",
    start_date=datetime(2024, 1, 2),
    catchup=False,
    default_args=DAG_DEFAULT_ARGS
)
def etl_dag():
    """ETL pipeline"""
    create_tables_tasks = []
    for i, query in enumerate(TABLES_CREATION_QUERY):
        task = SqliteOperator(
            task_id=f'create_table_{i}',
            sql=query,
            sqlite_conn_id='sqlite_etl'
        )
        create_tables_tasks.append(task)

        for i in range(len(create_tables_tasks) - 1):
            create_tables_tasks[i] 
    
    create_tables_tasks >> extract() >> transform() >> load()
etl_dag()
