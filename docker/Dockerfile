FROM apache/airflow:2.3.4
COPY ./config/requirements.txt /requirements.txt

USER airflow

RUN python -m pip install --upgrade pip
RUN pip install -r /requirements.txt --no-cache-dir



