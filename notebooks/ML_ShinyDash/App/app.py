import asyncio
from shiny import App, ui, render, reactive
import plotly.graph_objects as go
import pandas as pd
from databricks.sdk import config
from databricks import sql
import os

# Defined in `app.yaml`
SQL_WAREHOUSE_ID = os.getenv("DATABRICKS_WAREHOUSE_ID")
assert SQL_WAREHOUSE_ID, "DATABRICKS_WAREHOUSE_ID must be set in app.yaml."

# SQL query to fetch historical and predicted data
QUERY_HIST = """
SELECT fecha_corte, percent_on_time, NULL AS percent_on_time_lower, NULL AS percent_on_time_upper, 'Historical' AS type
FROM dbdemos.demo_payment_default.forecast_data
"""
QUERY_PRED = """
SELECT fecha_corte, percent_on_time, percent_on_time_lower, percent_on_time_upper, 'Predicted' AS type
FROM dbdemos.demo_payment_default.forecast_prediction_8088d57b
"""

# Function to execute SQL queries
def execute_query(connection, statement):
    try:
        with connection.cursor() as cursor:
            cursor.execute(statement)
            return cursor.fetchall_arrow().to_pandas()
    except Exception as e:
        print(f"Error executing query: {e}")
        raise e

# Function to create the Plotly plot
def create_plot(df_combined_pd):
    fig = go.Figure()

    # Add historical data as a line
    hist_data = df_combined_pd[df_combined_pd["type"] == "Historical"]
    fig.add_trace(go.Scatter(
        x=hist_data["fecha_corte"],
        y=hist_data["percent_on_time"],
        mode="lines",
        name="Historical",
        line=dict(color="blue")
    ))

    # Add predicted data as a line with points
    pred_data = df_combined_pd[df_combined_pd["type"] == "Predicted"]
    fig.add_trace(go.Scatter(
        x=pred_data["fecha_corte"],
        y=pred_data["percent_on_time"],
        mode="lines+markers",
        name="Predicted",
        line=dict(color="orange"),
        marker=dict(size=8, symbol="circle", color="orange")
    ))

    # Add confidence interval as a filled area
    fig.add_trace(go.Scatter(
        x=pd.concat([pred_data["fecha_corte"], pred_data["fecha_corte"][::-1]]),
        y=pd.concat([pred_data["percent_on_time_upper"], pred_data["percent_on_time_lower"][::-1]]),
        fill="toself",
        fillcolor="rgba(255, 165, 0, 0.2)",
        line=dict(color="rgba(255, 165, 0, 0)"),
        name="Confidence Interval"
    ))

    # Customize layout
    fig.update_layout(
        title="Historical and Predicted Percent of Payments on Time",
        xaxis_title="Fecha Corte",
        yaxis_title="Percent on Time",
        legend_title="Data Type",
        template="plotly_white"
    )
    return fig

# Shiny UI
app_ui = ui.page_navbar(
    ui.nav_panel(
        "Visualization",
        ui.output_plot("plotly_plot")
    ),
    title="Payment Prediction Visualization",
)

# Shiny Server
def server(input, output, session):
    cfg = config.Config()

    @reactive.Calc
    def fetch_data():
        try:
            connection = sql.connect(
                server_hostname=cfg.host,
                http_path=f"/sql/1.0/warehouses/{SQL_WAREHOUSE_ID}",
                credentials_provider=lambda: cfg.authenticate,
                session_configuration={"STATEMENT_TIMEOUT": "60"},
            )
            # Fetch data
            df_hist = execute_query(connection, QUERY_HIST)
            df_pred = execute_query(connection, QUERY_PRED)
            # Combine data
            return pd.concat([df_hist, df_pred], ignore_index=True)
        except Exception as e:
            ui.notification_show(f"Error fetching data: {e}", type="error")
            return pd.DataFrame()

    @render.plot
    def plotly_plot():
        df_combined_pd = fetch_data()
        if df_combined_pd.empty:
            return None
        return create_plot(df_combined_pd)

app = App(app_ui, server)

if __name__ == "__main__":
    app.run()
