import airflow
import datetime
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator

# this is a default properties defined as python dictionary
default_args_dict = {
    'start_date': airflow.utils.dates.days_ago(0), #current moment
    'concurrency': 1,           # how many tasks in a dag can occured simultaneously
    'schedule_interval': None,  # how often dag is going to be re-executed cyclically
                                # None means no repetitions
    'retries': 1,               # how many time a job is reatempted in case of failure
    'retry_delay': datetime.timedelta(minutes=5), # delay between tries
}

# define dag as a python object, identified with id and default properties
first_dag = DAG(
    dag_id='first_dag_stub',
    default_args=default_args_dict,
    catchup=False,  # in case of failure, we pause the dag, and when it restarts it goes to where it stops 
                    # Careful because if restart after long time, it will run for each delay it missed
                    # False prevent this. 
)

# callback function called when there is an error. Can acces contextual info savec within database

# BashOperator execute a batch command
task_one = BashOperator(
    task_id='get_spreadsheet',
    dag=first_dag,
    bash_command="curl https://www.lutemusic.org/spreadsheet.xlsx --output /opt/airflow/dags/{{ds_nodash}}.xlsx",
)#ds_nodash python template, substitute it with the current date. Name of file = current date
#curl ==> fownload


task_two = BashOperator(
    task_id='transmute_to_csv',
    dag=first_dag,
    trigger_rule='all_success', #executed only if all previous operator are successful
    bash_command="xlsx2csv /opt/airflow/dags/{{ds_nodash}}.xlsx > /opt/airflow/dags/{{ds_nodash}}_correct.csv",
)#xls2csv convert xlsx to csv

# Check for a particular line in the csv file we created
# then deletes it (because we know there is mistake)
task_three = BashOperator(
    task_id='time_filter',
    dag=first_dag,
    trigger_rule='all_success',
    bash_command="awk -F, 'int($31) > 1588612377' /opt/airflow/dags/{{ds_nodash}}_correct.csv > /opt/airflow/dags/{{ds_nodash}}_correct_filtered.csv",
)

task_four = BashOperator(
    task_id='load',
    dag=first_dag,
    trigger_rule='all_success',
    bash_command="echo \"loaded\"",
)

task_five = BashOperator(
    task_id='cleanup',
    dag=first_dag,
    bash_command="rm /opt/airflow/dags/{{ds_nodash}}_correct.csv /opt/airflow/dags/{{ds_nodash}}_correct_filtered.csv /opt/airflow/dags/{{ds_nodash}}.xlsx",
    )


task_one >> task_two >> task_three >> task_four >> task_five



