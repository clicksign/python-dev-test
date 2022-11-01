FROM apache/airflow:2.4.2-python3.9
USER root

USER ${AIRFLOW_UID:-50000} .
COPY requeriments.txt .
RUN pip3 install --upgrade pip
RUN pip3 install -r requeriments.txt