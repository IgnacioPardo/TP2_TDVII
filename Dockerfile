FROM python:3.11-bookworm

ENV AIRFLOW_HOME=/opt/airflow
ENV AIRFLOW_VERSION=2.9.1
WORKDIR /opt/airflow
RUN pip install poetry==1.4.2
COPY pyproject.toml poetry.lock /opt/airflow/
RUN poetry add prophet
RUN poetry add pyarrow
RUN poetry add fpdf2
RUN poetry add apache-airflow
RUN poetry lock && poetry export -f requirements.txt --output requirements.txt
RUN poetry config virtualenvs.create false && poetry install --no-root
RUN poetry install
RUN airflow db migrate && airflow users create --username airflow --firstname Peter --lastname Parker --role Admin --password airflow --email spiderman@superhero.org
COPY td7/ /opt/airflow/td7
COPY README.md .env /opt/airflow/
CMD airflow webserver --port 8080 & airflow scheduler
RUN psql -U $POSTGRES_USER -d $POSTGRES_DB -a -f /docker-entrypoint-initdb.d/01_create_tables.sql
