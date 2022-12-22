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
            ) as dag:
        task1 = BashOperator(
        task_id = "task1",
        bash_command = f'echo hello from Airflow {datetime.now()} >> /home/hadoop/apacheAirflowTest1.txt'
        )
        task1


# In[ ]:




