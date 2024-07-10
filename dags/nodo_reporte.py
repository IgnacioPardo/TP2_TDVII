""" Module for the last day of the month node of the DAG """

# pylint: disable=E0401

import logging

from fpdf import FPDF  # type: ignore
import pandas as pd

from sqlalchemy import create_engine  # type: ignore
from sqlalchemy import text  # type: ignore
from sqlalchemy.orm import sessionmaker  # type: ignore

logger = logging.getLogger(__name__)
# from td7.config import POSTGRES_CONN_STRING

POSTGRES_USER = "douglas_adams"
POSTGRES_PASSWORD = "hitchhiker"
POSTGRES_DB = "postgres"

POSTGRES_CONN_STRING = (
    f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@postgres/{POSTGRES_DB}"
)

pg = create_engine(POSTGRES_CONN_STRING)

Session = sessionmaker(bind=pg)


def get_total_volume(month_offset):
    result = pg.execute(
        text(
            """
            SELECT
                SUM(monto) AS volumen_total
            FROM
                Transaccion
            WHERE
                fecha >= :lower_fecha
                AND fecha < :upper_fecha
            """
        ),
        lower_fecha=pd.Timestamp.now().date() - pd.DateOffset(months=month_offset),
        upper_fecha=pd.Timestamp.now().date() + pd.DateOffset(months=month_offset),
    )

    row = result.fetchone()

    return row.volumen_total if row else 0


def get_transaction_count_by_type(month_offset):
    """ Get the transaction count by type"""

    result = pg.execute(
        text(
            """
            SELECT
                es_con_tarjeta,
                COUNT(*) AS cantidad_transacciones
            FROM
                Transaccion
            WHERE
                fecha >= :lower_fecha
                AND fecha < :upper_fecha
            GROUP BY
                es_con_tarjeta
            """
        ),
        lower_fecha=pd.Timestamp.now().date() - pd.DateOffset(months=month_offset),
        upper_fecha=pd.Timestamp.now().date() + pd.DateOffset(months=month_offset),
    )

    res = result.fetchall()
    transactions = {row.es_con_tarjeta: row.cantidad_transacciones for row in res}

    return transactions


def get_new_accounts(month_offset):
    """ Get the number of new accounts in the last month"""

    result = pg.execute(
        text(
            """
            SELECT
                COUNT(*) AS cantidad_cuentas_nuevas
            FROM
                Usuario
            WHERE
                fecha_alta >= :lower_fecha
                AND fecha_alta < :upper_fecha
            """
        ),
        lower_fecha=pd.Timestamp.now().date() - pd.DateOffset(months=month_offset),
        upper_fecha=pd.Timestamp.now().date() + pd.DateOffset(months=month_offset),
    )

    row = result.fetchone()

    return row.cantidad_cuentas_nuevas if row else 0


def get_income_expense(month_offset):
    """ Get the total income and expense in the last month """

    result = pg.execute(
        text(
            """
            SELECT
                SUM(CASE WHEN c_origen.esVirtual AND NOT c_destino.esVirtual THEN monto ELSE 0 END) AS egreso_total,
                SUM(CASE WHEN NOT c_origen.esVirtual AND c_destino.esVirtual THEN monto ELSE 0 END) AS ingreso_total
            FROM
                Transaccion t
            INNER JOIN Clave c_origen ON (t.CU_Origen = c_origen.clave_Uniforme)
            INNER JOIN Clave c_destino ON (t.CU_Destino = c_destino.clave_Uniforme)
            WHERE
                fecha >= :lower_fecha
                AND fecha < :upper_fecha
            """
        ),
        lower_fecha=pd.Timestamp.now().date() - pd.DateOffset(months=month_offset),
        upper_fecha=pd.Timestamp.now().date() + pd.DateOffset(months=month_offset),
    )

    row = result.fetchone()

    return {"ingreso_total": row.ingreso_total if row else 0, "egreso_total": row.egreso_total if row else 0}


def get_credit_card_interest(month_offset):
    """ Get the total interest earned from credit cards in the last month """

    result = pg.execute(
        text(
            """
            SELECT
                SUM(monto * interes) AS intereses_ganados
            FROM
                Transaccion
            WHERE
                es_con_tarjeta = TRUE
                AND fecha >= :lower_fecha
                AND fecha < :upper_fecha
            """
        ),
        lower_fecha=pd.Timestamp.now().date() - pd.DateOffset(months=month_offset),
        upper_fecha=pd.Timestamp.now().date() + pd.DateOffset(months=month_offset),
    )

    row = result.fetchone()

    return row.intereses_ganados if row else 0


def generate_pdf_report(data):
    """ Generate a PDF report with the data

    Args:
        data (dict):  data = {
            "total_volume": [total_volume, total_volume_change],
            "total_transactions": [total_transactions, total_transactions_change],
            "new_accounts": [new_accounts, new_accounts_change],
            "net_balance": [net_balance, net_balance_change],
            "total_interest": [total_interest, total_interest_change],
        }
    """

    pdf = FPDF()
    # ln is DEPRECATED

    pdf.add_page()
    pdf.set_font("Helvetica", size=12)

    # Title
    pdf.cell(200, 10, f"**Monthly Report** - {pd.Timestamp.now().date()}", align="C", markdown=True)

    # Total volume
    pdf.ln(10)
    pdf.cell(200, 10, "**Transactioned Volume**", align="L", markdown=True)
    pdf.ln(10)
    pdf.cell(200, 10, f"Total volume: $ {data['total_volume'][0]:,}", align="L")
    pdf.ln(10)
    pdf.cell(200, 10, f"Monthly Variation: {data['total_volume'][1]:.2%}", align="L")

    # Total transactions
    pdf.ln(20)
    pdf.cell(200, 10, "**Transaction Count**", align="L", markdown=True)
    pdf.ln(10)
    pdf.cell(200, 10, "Total transactions", align="L", markdown=True)
    for key, value in data["total_transactions"][0].items():
        pdf.ln(10)
        pdf.cell(200, 10, f"        With Card: {key}: {value:,}", align="L")
    pdf.ln(10)
    pdf.cell(200, 10, "Monthly Variation", align="L", markdown=True)
    for key, value in data["total_transactions"][1].items():
        pdf.ln(10)
        pdf.cell(200, 10, f"        With Card: {key}: {value:.2%}", align="L")

    # New accounts
    pdf.ln(20)
    pdf.cell(200, 10, f"**New accounts**: {data['new_accounts'][0]:,}", align="L", markdown=True)
    pdf.ln(10)
    pdf.cell(200, 10, f"Monthly Variation: {data['new_accounts'][1]:.2%}", align="L")

    # Net balance
    pdf.ln(20)
    pdf.cell(200, 10, "**Net balance**", align="L", markdown=True)
    for key, value in data["net_balance"][0].items():
        pdf.ln(10)
        pdf.cell(200, 10, f"        {key}: $ {value:,}", align="L")
    pdf.ln(10)
    pdf.cell(200, 10, "Monthly Variation", align="L", markdown=True)
    for key, value in data["net_balance"][1].items():
        pdf.ln(10)
        pdf.cell(200, 10, f"        {key}: {value:.2%}", align="L")

    # Total interest
    pdf.ln(20)
    pdf.cell(200, 10, "**Total interest**", align="L", markdown=True)
    pdf.ln(10)
    pdf.cell(200, 10, f"Total interest: $ {data['total_interest'][0]:,}", align="L")
    pdf.ln(10)
    pdf.cell(200, 10, f"Monthly Variation: {data['total_interest'][1]:.2%}", align="L")

    pdf.output("monthly_report.pdf")


def generate_monthly_report():
    """ Generate the monthly report """

    # Volumen total transaccionado 
    # Cantidad de transacciones por tipo
    # Cantidad de cuentas nuevas
    # Ingreso/egreso total
    # Intereses ganados por tarjetas de credito

    # este mes y cambio porcentual con el anterior

    total_volume = get_total_volume(1)
    total_volume_change = get_total_volume(2) / total_volume if total_volume > 0 else 0

    total_transactions = get_transaction_count_by_type(1)
    total_transactions_change = {key: value / total_transactions[key] for key, value in get_transaction_count_by_type(2).items()} if total_transactions else 0

    new_accounts = get_new_accounts(1)
    new_accounts_change = get_new_accounts(2) / new_accounts if new_accounts > 0 else 0

    net_balance = get_income_expense(1)
    net_balance_change = {
        key: (
            value / net_balance[key]
            if net_balance[key] != 0
            else 0
        )
        for key, value in get_income_expense(2).items()
    } if net_balance else 0

    total_interest = get_credit_card_interest(1)

    total_interest_change = (
        get_credit_card_interest(2) / total_interest
        if total_interest > 0
        else 0
    )

    data = {
        "total_volume": [total_volume, total_volume_change],
        "total_transactions": [total_transactions, total_transactions_change],
        "new_accounts": [new_accounts, new_accounts_change],
        "net_balance": [net_balance, net_balance_change],
        "total_interest": [total_interest, total_interest_change],
    }

    # Log the report
    logger.info("Monthly report: %s", data)

    # Save report as pdf

    generate_pdf_report(data)

    return data
