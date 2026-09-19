FROM apache/airflow:2.9.3-python3.11

COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir --default-timeout=300 --retries=10 -r /requirements.txt