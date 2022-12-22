#!/usr/bin/env python
# coding: utf-8

# In[5]:


from airflow import DAG
from datetime import datetime,timedelta
from airflow.operators.bash import BashOperator


# In[6]:


default_args = {
    'owner' : 'omar',
    'retries' : 3,
    'retry_delay' : timedelta(minutes=2)
}


# In[17]:


with DAG(
        dag_id = 'my_first_dag_bash',
        default_args = default_args,
        description = 'first dag for BD training',
        start_date=datetime(2022, 12, 22, 10, 15),
        schedule= "* * * * *",
        catchup = False
             task1 = BashOperator(
        task_id = "task1",
        bash_command= f'echo hello from Airflow {datetime.now()} >> /home/hadoop/airflow/apacheAirflow',
    )

    task2 = BashOperator(
        task_id = "task2",
        bash_command= f'echo secondary Airflow task {datetime.now()} >> /home/hadoop/airflow/apacheAirflow',
    )

    task3 = BashOperator(
        task_id = "task3",
        bash_command= f'echo 3rd Airflow task- {datetime.now()} >> /home/hadoop/airflow/apacheAirflow',
    )
    )



    task1 >> task2 >> task3


# In[ ]:




