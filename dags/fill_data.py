"""DAG para llenar la base de datos con datos de prueba."""

# pylint: disable=E0401
# pylint: disable=W0212

from airflow import DAG  # type: ignore
from airflow.operators.python import PythonOperator  # type: ignore
from airflow.operators.python import BranchPythonOperator  # type: ignore
from airflow.operators.generic_transfer import GenericTransfer  # type: ignore
import pendulum  # type: ignore

from nodos import transaction_volume, nodo3, get_data, forecast_transacciones

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

    # Primer OP: Generar datos
    # Se crean cuentas
    # de forma diaria se simula:
    # Se deposita dinero en las cuentas
    # Se transacciona entre las cuentas
    # Se pone plata a rendir
    # Se pagan los rendimientos

    op = PythonOperator(
        task_id="data_generator",
        python_callable=generate_data,
        op_kwargs=dict(timespan=20),
    )

    # Segundo OP: Branch Operator
    # Si es el último día del mes, se cuentan las cuentas
    # - Saldo total al final del mes
    # - Cantidad de cuentas nuevas
    # - Volumen de transacciones generadas
    # - Total de rendimientos pagados
    # Cada día se cuentan los rendimientos pagados

    branch_op = BranchPythonOperator(
        task_id="branch",
        python_callable=(
            lambda: (
                "nodo3"
                if pendulum.now()._last_of_month() == pendulum.now()
                else "transaction_volume"
            )
        ),
    )

    transaction_vol_op = PythonOperator(
        task_id="transaction_volume",
        python_callable=transaction_volume,
    )

    op3 = PythonOperator(
        task_id="nodo3",
        python_callable=nodo3,
    )

    _ = op >> branch_op >> [transaction_vol_op, op3]
