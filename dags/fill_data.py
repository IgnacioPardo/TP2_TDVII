"""DAG para llenar la base de datos con datos de prueba."""

# pylint: disable=E0401
# pylint: disable=W0212

from airflow import DAG  # type: ignore
from airflow.operators.python import PythonOperator  # type: ignore
# from airflow.operators.python import BranchPythonOperator  # type: ignore
from airflow.operators.datetime import BranchDateTimeOperator  # type: ignore
from airflow.operators.dummy import DummyOperator  # type: ignore
from airflow.operators.email import EmailOperator  # type: ignore
import pendulum  # type: ignore

from nodo_tna_update import predict_transaction_volume_update_tna
from nodo_reporte import generate_monthly_report

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
        op_kwargs=dict(timespan=40),
    )

    transaction_vol_op = PythonOperator(
        task_id="transaction_volume_forecast",
        python_callable=predict_transaction_volume_update_tna,
    )

    branch_op = BranchDateTimeOperator(
        task_id="branch_ultimo_dia_mes",
        follow_task_ids_if_true="ultimo_dia_mes",
        follow_task_ids_if_false="dummy",
        # target_upper=pendulum.now()._last_of_month() + pendulum.duration(days=1),
        # target_lower=pendulum.now()._last_of_month() - pendulum.duration(days=1),
        target_upper=pendulum.now() + pendulum.duration(days=1),
        target_lower=pendulum.now() - pendulum.duration(days=1),
    )

    monthly_report = PythonOperator(
        task_id="ultimo_dia_mes",
        python_callable=generate_monthly_report,
    )

    dummy_op = DummyOperator(task_id="dummy")

    email_report_op = EmailOperator(
        task_id="email_report",
        to="ipardo@mail.utdt.edu",
        cc="jsilvestri@mail.utdt.edu",
        subject="Reporte mensual",
        html_content=f"""
        <h3>Reporte mensual</h3>
        <p>Se adjunta el reporte mensual para el mes: {pendulum.now().format("MMMM YYYY")}</p>

        <p>MercadoPago</p>
        <footer> Este es un mensaje automático. Por favor no responder a este correo. </footer>
        """,
        files=["monthly_report.pdf"],
    )

    _ = op >> transaction_vol_op
    _ = op >> branch_op >> [monthly_report, dummy_op]
    _ = monthly_report >> email_report_op
