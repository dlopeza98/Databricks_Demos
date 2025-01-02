# Databricks notebook source
from pyspark.sql import functions as F
import pandas as pd

df_time_hist = spark.read.table("dbdemos.demo_payment_default.forecast_data")
df_time_hist = df_time_hist.withColumn("type", F.lit("Historical"))
df_time_hist.display()

# COMMAND ----------

df_time_pred = spark.read.table("dbdemos.demo_payment_default.forecast_prediction_8088d57b")
df_time_pred  = df_time_pred.withColumn("type", F.lit("Predicted"))
df_time_pred.display()

# COMMAND ----------

# Combine the DataFrames
# Select relevant columns and union the DataFrames
df_combined = (
    df_time_hist.select("fecha_corte", "percent_on_time", F.lit(None).alias("percent_on_time_lower"), F.lit(None).alias("percent_on_time_upper"), "type")
    .unionByName(
        df_time_pred.select("fecha_corte", "percent_on_time", "percent_on_time_lower", "percent_on_time_upper", "type")
    )
)

# Convert to Pandas for plotting
df_combined_pd = df_combined.toPandas()

import plotly.graph_objects as go

# Initialize the plot
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
    mode="lines+markers",  # Add markers to the line
    name="Predicted",
    line=dict(color="orange"),
    marker=dict(size=8, symbol="circle", color="orange")  # Customize marker size and color
))

# Add confidence interval as a filled area
fig.add_trace(go.Scatter(
    x=pd.concat([pred_data["fecha_corte"], pred_data["fecha_corte"][::-1]]),  # Upper then Lower for shading
    y=pd.concat([pred_data["percent_on_time_upper"], pred_data["percent_on_time_lower"][::-1]]),
    fill="toself",
    fillcolor="rgba(255, 165, 0, 0.2)",  # Orange with transparency
    line=dict(color="rgba(255, 165, 0, 0)"),  # No line
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

# Show the plot
fig.show()

# COMMAND ----------

# MAGIC %pip install shiny
# MAGIC

# COMMAND ----------

# MAGIC %sh shiny run --app-dir App --host 0.0.0.0 --port 8080
# MAGIC
