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


def nodo3():
    pass


if __name__ == "__main__":
    predict_transaction_volume_update_tna()
