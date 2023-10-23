import airflow
import datetime
import pandas as pd
from faker import Faker
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator
from random import randint
import requests

default_args_dict = {
    'start_date': airflow.utils.dates.days_ago(0),
    'concurrency': 1,
    'schedule_interval': "0 0 * * 0", #Run once a week at midnight on Sunday morning
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=5),
}

assgnement_dag = DAG(
    dag_id='assgnement_dag',
    default_args=default_args_dict,
    catchup=False,
    template_searchpath=['/opt/airflow/dags/']
)

def _create_random_name_attributes(output_folder, epoch):
    faker = Faker() #random name
    list_name = []
    list_attributes = []
    
    for i in range(5):
        list_name.append(faker.name())
        attributes = [randint(6,18), randint(2,18), randint(2,18), randint(2,18), randint(2,18), randint(2,18)]
        print(attributes)
        list_attributes.append(attributes)
    
    print(list_attributes)

    df = pd.DataFrame({'name':list_name, 'attributes': list_attributes})

    csv_path = output_folder + "/" + epoch + ".csv"
    df.to_csv(csv_path, index=False)

task_one = PythonOperator(
    task_id='create_random_name',
    dag=assgnement_dag,
    python_callable=_create_random_name_attributes,
    op_kwargs={
        "output_folder": "/opt/airflow/dags/assignment",
        "epoch": "{{ execution_date.int_timestamp }}",
    },
    trigger_rule='all_success',
    depends_on_past=False,
)

def _call_api_races_language(output_folder, epoch, url):
    
    response = requests.get(url)
    response_json = response.json()
    count = response_json["count"]
    print(count)

    list_race = []
    list_languages = []

    for i in range(5):
        #race
        race = response_json["results"][randint(0,count-1)]["index"]
        print(race)
        list_race.append(race)

        #languages
        json_languages = requests.get(url + "/" + race).json()
        languages = ""

        for language in json_languages["languages"] :
            print(language)
            print(language["index"])
            print(str((language["index"])))
            languages = languages + str(language["index"]) + " "

        print(languages)
        list_languages.append(languages)
        
    df = pd.read_csv(f'{output_folder}/{str(epoch)}.csv')

    df["Race"] = list_race
    df["Language"] = list_languages

    csv_path = output_folder + "/" + epoch + ".csv"
    df.to_csv(csv_path, index=False)

    
task_two = PythonOperator(
    task_id='call_api_races',
    dag=assgnement_dag,
    python_callable=_call_api_races_language,
    op_kwargs={
        "output_folder": "/opt/airflow/dags/assignment",
        "epoch": "{{ execution_date.int_timestamp }}",
        "url": "https://www.dnd5eapi.co/api/races"
    },
    trigger_rule='all_success',
    depends_on_past=False, 
)

def _create_query(output_folder, epoch):
    df = pd.read_csv(f'{output_folder}/{str(epoch)}.csv')
    print("begin")
    with open("/opt/airflow/dags/assignment/character.sql", "w") as f:
        print("opened")
        df_iterable = df.iterrows()
        print("before write")
        f.write(
            "CREATE TABLE IF NOT EXISTS character (\n"
            "name VARCHAR(255),\n"
            "attributes VARCHAR(255),\n"
            "race VARCHAR(255),\n"
            "language VARCHAR(255));\n"
        )
        print("after write")
        for index, row in df_iterable:
            name = row['name']
            attributes = row['attributes']
            Race = row['Race']
            Language = row['Language']

            print(name + " " + attributes + " " + Race + " " + Language)

            f.write(
                "INSERT INTO character VALUES ("
                f"'{name}', '{attributes}', '{Race}', '{Language}'"
                ");\n"
            )

            print(row + "written")

        f.close()


task_three = PythonOperator(
    task_id='create_query',
    dag=assgnement_dag,
    python_callable=_create_query,
    op_kwargs={
        "output_folder": "/opt/airflow/dags/assignment",
        "epoch": "{{ execution_date.int_timestamp }}",
    },
    trigger_rule='all_success',
)


task_four = PostgresOperator(
    task_id='insert_query',
    dag=assgnement_dag,
    postgres_conn_id='postgres_default', #changed
    sql='character.sql',
    trigger_rule='all_success',
    autocommit=True
)

end = DummyOperator(
    task_id='end',
    dag=assgnement_dag,
    trigger_rule='none_failed'
)

task_one >> task_two >> task_three >> task_four>> end
