"""DAG para llenar la base de datos con datos de prueba."""

# pylint: disable=E0401

from airflow import DAG  # type: ignore
from airflow.operators.python import PythonOperator  # type: ignore
import pendulum  # type: ignore

# import datetime
# from td7.data_generator import DataGenerator
# from td7.schema import Schema

from td7.datagen import generate_data


with DAG(
    "fill_data",
    start_date=pendulum.datetime(2024, 6, 1, tz="UTC"),
    schedule_interval="@daily",
    catchup=True,
) as dag:
    op = PythonOperator(
        task_id="data_generator",
        python_callable=generate_data,
        # op_kwargs=dict(),
    )

    # Primer OP: Generar datos
    # Se crean cuentas
    # de forma diaria se simula:
    # Se deposita dinero en las cuentas
    # Se transacciona entre las cuentas
    # Se pone plata a rendir
    # Se pagan los rendimientos

    # Segundo OP: Branch Operator
    # Si es el último día del mes, se cuentan las cuentas
    # - Saldo total al final del mes
    # Cada día se cuentan los rendimientos pagados

    # - Se cuentan los rendimientos pagados por día
    # - Se cuentan las transacciones por día
    # - Ver queries.sql ...
