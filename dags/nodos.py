from prophet import Prophet  # type: ignore
import pandas as pd
from matplotlib import pyplot as plt

from sqlalchemy import create_engine  # type: ignore
from sqlalchemy import text   # type: ignore
from sqlalchemy.orm import sessionmaker   # type: ignore

import logging

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


# Read transactions from the last n (40) days
# Calculate volume of transactions per day
# Predict volume of transactions for the next 7 days


def get_data():
    with pg.connect() as conn:
        with conn.begin():
            result = conn.execute(
                text(
                    """
                    SELECT sum(
                        t.monto * case when c.esvirtual then -1 else 1 end
                    ) as volume, date(t.fecha) as day
                    FROM Transaccion t
                    INNER JOIN Clave c ON (t.cu_origen = c.clave_uniforme)
                    INNER JOIN Clave c2 ON (t.cu_destino = c2.clave_uniforme)
                    WHERE c.esvirtual != c2.esvirtual
                    GROUP BY day
                    ORDER BY day DESC
                    LIMIT 30
                    """
                )
            )

            res = result.fetchall()

            data = pd.DataFrame(res, columns=["y", "ds"])

            # if there is less than 30 days of data, return None
            if len(data) < 30:
                return None

        return data


def forecast_transacciones(data: pd.DataFrame):
    model = Prophet()

    # Remove extreme values por la generación random de datos
    data = data[data["y"] < data["y"].quantile(0.92)]

    model.fit(data)
    future = model.make_future_dataframe(periods=7)
    forecast = model.predict(future)

    # Plot the forecast
    fig, ax = plt.subplots(figsize=(10, 5))
    ax.set_title("Forecast")

    model.plot(forecast, ax=ax)

    plt.xticks(rotation=25)
    # save plot to file
    fig.savefig("forecast.png")

    fig_comp = model.plot_components(forecast)
    fig_comp.savefig("forecast_components.png")

    return forecast


def calc_variation(data, forecast):
    """ Calculate the variation between the forecast and the real data

    Args:
        data (pd.DataFrame): Data from the last 30 days (y: volume of transactions, ds: date)
        forecast (pd.DataFrame): Forecast for the next 7 days

    Returns:
        tuple: variation, pe
    """

    # Calculate the variation between the forecast and the real data
    # Calculate the mean absolute percentage error

    # Variation 7 days

    # average last 7 days of data
    data_last_week = data["y"].tail(7).mean()

    # average first 7 days of forecast after the last day of data
    forecast_wo_data_first_week = forecast[forecast["ds"] > pd.to_datetime(data["ds"].max())]["yhat_lower"].head(7).mean()

    variation_7days = forecast_wo_data_first_week - data_last_week
    pe_7days = variation_7days / data_last_week

    if pe_7days < 0:
        pe_7days = 0

    # Variation next day
    # get last day of data
    data_last_day = data["y"].tail(1).values[0]

    # get first day of forecast after the last day of data
    forecast_wo_data_first_day = forecast[forecast["ds"] > pd.to_datetime(data["ds"].max())]["yhat_lower"].head(1).values[0]

    variation_next_day = forecast_wo_data_first_day - data_last_day
    pe_next_day = variation_next_day / data_last_day

    if pe_next_day < 0:
        pe_next_day = 0

    # Optimista
    best_pe = max(pe_7days, pe_next_day)
    best_variation = max(variation_7days, variation_next_day)

    return best_variation, best_pe


def update_tna(pe):
    """ Update the TNA for tomorrow

    Args:
        pe (float): Percentage error
    """

    with pg.connect() as conn:
        with conn.begin():
            result = conn.execute(
                text(
                    """
                    SELECT valor, fecha
                    FROM HistoricalTNA
                    WHERE fecha = current_date
                    """
                )
            )

            res = result.fetchone()

            if res is None:
                return None, None

            # Update TNA for tomorrow
            tna = res["valor"]
            fecha = res["fecha"]

            logger.info("TNA for today: %s", tna)

            tna_tomorrow = tna * pe
            tna_tomorrow = min(0.5, tna_tomorrow)

            # Check if TNA is allready set for tomorrow
            result = conn.execute(
                text(
                    """
                    SELECT valor
                    FROM HistoricalTNA
                    WHERE fecha = :fecha
                    """
                ),
                fecha=fecha + pd.Timedelta(days=1),
            )

            res = result.fetchone()
            if res is not None:
                return tna, res["valor"]

            conn.execute(
                text(
                    """
                    INSERT INTO HistoricalTNA (fecha, valor)
                    VALUES (:fecha, :valor)
                    """
                ),
                fecha=fecha + pd.Timedelta(days=1),
                valor=tna_tomorrow,
            )

            return tna, tna_tomorrow


def predict_transaction_volume_update_tna():
    """ Calculate the forecast of transaction volume for the next 7 days.
        Update investment TNA for tomorrow.

    Returns:
        pd.DataFrame: Forecast for the next 7 days
    """

    data = get_data()
    if data is None:
        return None

    forecast = forecast_transacciones(data)
    # Log the forecast
    logger.info("Forecast: %s", forecast)

    _, pe = calc_variation(data, forecast)

    # Log the percentage error
    logger.info("Percentage error: %s", pe)

    if pe > 0:
        tna, tna_t = update_tna(pe)

        # Log the TNA
        logger.info("TNA: %s", tna)

        # Log the updated TNA
        logger.info("Updated TNA: %s", tna_t)

    return forecast, pe


def get_total_volume(month_offset):
    with pg.connect() as conn:
        with conn.begin():
            result = conn.execute(
                text(
                    """
                    SELECT 
                        SUM(monto) AS volumen_total
                    FROM 
                        Transaccion
                    WHERE 
                        fecha BETWEEN date_trunc('month', CURRENT_DATE) - INTERVAL :month_offset || ' month' 
                        AND date_trunc('month', CURRENT_DATE) - INTERVAL :month_offset || ' month' + INTERVAL '1 month' - INTERVAL '1 day'
                    """
                ),
                {"month_offset": month_offset}
            )

            row = result.fetchone()

            return row.volumen_total if row else 0

        
def get_transaction_count_by_type(month_offset):
    with pg.connect() as conn:
        with conn.begin():
            result = conn.execute(
                text(
                    """
                    SELECT 
                        es_con_tarjeta,
                        COUNT(*) AS cantidad_transacciones
                    FROM 
                        Transaccion
                    WHERE 
                        fecha BETWEEN date_trunc('month', CURRENT_DATE) - INTERVAL :month_offset || ' month' 
                        AND date_trunc('month', CURRENT_DATE) - INTERVAL :month_offset || ' month' + INTERVAL '1 month' - INTERVAL '1 day'
                    GROUP BY 
                        es_con_tarjeta
                    """
                ),
                {"month_offset": month_offset}
            )

            res = result.fetchall()
            transactions = {row.es_con_tarjeta: row.cantidad_transacciones for row in res}

            return transactions
        
def get_new_accounts(month_offset):
    with pg.connect() as conn:
        with conn.begin():
            result = conn.execute(
                text(
                    """
                    SELECT 
                        COUNT(*) AS cantidad_cuentas_nuevas
                    FROM 
                        Usuario
                    WHERE 
                        fecha_alta BETWEEN date_trunc('month', CURRENT_DATE) - INTERVAL :month_offset || ' month' 
                        AND date_trunc('month', CURRENT_DATE) - INTERVAL :month_offset || ' month' + INTERVAL '1 month' - INTERVAL '1 day'
                    """
                ),
                {"month_offset": month_offset}
            )

            row = result.fetchone()

            return row.cantidad_cuentas_nuevas if row else 0

def get_income_expense(month_offset):
    with pg.connect() as conn:
        with conn.begin():
            result = conn.execute(
                text(
                    """
                    SELECT 
                        SUM(CASE WHEN c_origen.esVirtual AND NOT c_destino.esVirtual THEN monto ELSE 0 END) AS egreso_total,
                        SUM(CASE WHEN NOT c_origen.esVirtual AND c_destino.esVirtual THEN monto ELSE 0 END) AS ingreso_total
                    FROM 
                        Transaccion t INNER JOIN Clave c_origen ON t.CU_Origen = c.clave_Uniforme 
                                      INNER JOIN Clave c_destino ON t.CU_Destino = c_destino.clave_Uniforme
                    WHERE 
                        fecha BETWEEN date_trunc('month', CURRENT_DATE) - INTERVAL :month_offset || ' month' 
                        AND date_trunc('month', CURRENT_DATE) - INTERVAL :month_offset || ' month' + INTERVAL '1 month' - INTERVAL '1 day'
                    """
                ),
                {"month_offset": month_offset}
            )

            row = result.fetchone()

            return {"ingreso_total": row.ingreso_total if row else 0, "egreso_total": row.egreso_total if row else 0}

def get_credit_card_interest(month_offset):
    with pg.connect() as conn:
        with conn.begin():
            result = conn.execute(
                text(
                    """
                    SELECT 
                        SUM(monto * interes) AS intereses_ganados
                    FROM 
                        Transaccion
                    WHERE 
                        es_con_tarjeta = TRUE
                        AND fecha BETWEEN date_trunc('month', CURRENT_DATE) - INTERVAL :month_offset || ' month' 
                        AND date_trunc('month', CURRENT_DATE) - INTERVAL :month_offset || ' month' + INTERVAL '1 month' - INTERVAL '1 day'
                    """
                ),
                {"month_offset": month_offset}
            )

            row = result.fetchone()

            return row.intereses_ganados if row else 0


def generate_monthly_report():
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
    net_balance_change = {key: value / net_balance[key] for key, value in get_income_expense(2).items()} if net_balance else 0

    total_interest = get_credit_card_interest(1)
    total_interest_change = get_credit_card_interest(2) / total_interest if total_interest > 0 else 0

    dataframe = pd.DataFrame(
        {
            "total_volume": [total_volume, total_volume_change],
            "total_transactions": [total_transactions, total_transactions_change],
            "new_accounts": [new_accounts, new_accounts_change],
            "net_balance": [net_balance, net_balance_change],
            "total_interest": [total_interest, total_interest_change],
        },
    )

    return dataframe


if __name__ == "__main__":
    predict_transaction_volume_update_tna()
