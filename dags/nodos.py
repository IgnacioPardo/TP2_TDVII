from prophet import Prophet
import pandas as pd
from matplotlib import pyplot as plt

from sqlalchemy import create_engine  # type: ignore
from sqlalchemy import text   # type: ignore
from sqlalchemy.orm import sessionmaker   # type: ignore

from airflow import DAG, task  # type: ignore

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


def transaction_volume():
    
    data = get_data()
    forecast = forecast_transacciones(data)

    return forecast


def nodo3():
    pass


if __name__ == "__main__":
    transaction_volume()
