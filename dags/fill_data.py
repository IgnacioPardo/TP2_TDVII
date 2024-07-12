"""DAG para llenar la base de datos con datos de prueba."""

# pylint: disable=E0401
# pylint: disable=W0212
import logging

from airflow import DAG  # type: ignore
from airflow.operators.python import PythonOperator  # type: ignore
from airflow.operators.python import BranchPythonOperator  # type: ignore
from airflow.operators.datetime import BranchDateTimeOperator  # type: ignore
from airflow.operators.dummy import DummyOperator  # type: ignore
from airflow.operators.email import EmailOperator  # type: ignore
import pendulum  # type: ignore

from nodo_tna_update import predict_transaction_volume_update_tna
from nodo_reporte import generate_monthly_report

from td7.datagen import generate_data

logger = logging.getLogger(__name__)

# Si la start_date es ayer,
# se puede pasar el timespan al generator
# y se generan los datos desde timespan dias antes hasta ayer.

# Otra opcion es setear la start_date a hoy - timespan dias
# y al generator pasarle 1, para que cada dia genere un dia de datos.

SIM_TIMESPAN = 60
START_DATE = pendulum.now()

SIM_OPTION = "AIRFLOW"
# SIM_OPTION = "FOR-LOOP"  # Cambiar a FOR-LOOP para testear sin Airflow

if SIM_OPTION == "AIRFLOW":
    START_DATE = pendulum.now() - pendulum.duration(days=SIM_TIMESPAN)
    SIM_TIMESPAN = 1

GENERATOR_KWARGS = {
    "start_date": START_DATE,
}
if SIM_OPTION == "FOR-LOOP":
    GENERATOR_KWARGS = {"timespan": SIM_TIMESPAN}


def sim_branch_fn(logical_date, ds):
    """Determina si es el último día del mes en simulación"""

    logger.info("Logical date: %s", logical_date)
    logger.info("DS: %s", ds)
    logger.info("Last of month: %s", logical_date._last_of_month())

    return (
        "generate_monthly_report"
        if logical_date == logical_date._last_of_month()
        or ds == logical_date._last_of_month().format("YYYY-MM-DD")
        else "dummy"
    )


with DAG(
    "fill_data",
    start_date=START_DATE,
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
        op_kwargs=GENERATOR_KWARGS,
    )

    transaction_vol_op = PythonOperator(
        task_id="transaction_volume_forecast",
        python_callable=predict_transaction_volume_update_tna,
    )

    branch_op = BranchDateTimeOperator(
        task_id="branch_ultimo_dia_mes",
        follow_task_ids_if_true="generate_monthly_report",
        follow_task_ids_if_false="sim_branch",
        target_upper=pendulum.now()._last_of_month() + pendulum.duration(days=1),
        target_lower=pendulum.now()._last_of_month() - pendulum.duration(days=1),
        # Para testear sin esperar al último día del mes
        # target_upper=pendulum.now() + pendulum.duration(days=1),
        # target_lower=pendulum.now() - pendulum.duration(days=1),
    )

    # Si se está simulando el paso del tiempo a partir de la start_date del DAG
    # Utilizamos la fecha lógica del DAG para determinar si es el último día del mes
    # Y replicar lo que haría el BranchDateTimeOperator
    sim_branch_op = BranchPythonOperator(
        task_id="sim_branch",
        python_callable=sim_branch_fn,
    )

    monthly_report = PythonOperator(
        task_id="generate_monthly_report",
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
    _ = op >> branch_op >> [monthly_report, sim_branch_op]
    _ = sim_branch_op >> [monthly_report, dummy_op]
    _ = monthly_report >> email_report_op
