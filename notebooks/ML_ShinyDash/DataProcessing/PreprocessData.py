# Databricks notebook source
# MAGIC %md
# MAGIC # Data Preprocessing

# COMMAND ----------

# MAGIC %md
# MAGIC ## Preprocess raw historic data

# COMMAND ----------

# libraries
from pyspark.sql import functions as F
from pyspark.sql.functions import col

# COMMAND ----------

# Parameters
catalog_name = "dbdemos"
schema_name = "demo_payment_default"
table_raw_data = f"{catalog_name}.{schema_name}.raw_data"
table_refined_data = f"{catalog_name}.{schema_name}.refined_data_to_clasification_model"

# COMMAND ----------

# Step 1: Read data from the raw table
df_raw = spark.read.table(table_raw_data)
print(df_raw.count())
df_raw.limit(3).display()

# COMMAND ----------

# Step 2: Format data types, filtering data and columns
df_refined = (
    df_raw
    .withColumn("monthly_income", col("monthly_income").cast("double"))
    .withColumn("age", col("age").cast("int"))
    .withColumn("employment_years", col("employment_years").cast("int"))
    .withColumn("loan_amount", col("loan_amount").cast("double"))
    .withColumn("credit_score", col("credit_score").cast("int"))
    .withColumn("payment_on_time", col("payment_on_time").cast("string"))
    .select("fecha_corte", "monthly_income", "age", "employment_years", "loan_amount", "credit_score", "payment_on_time")
    .filter((col("age") >= 18) & (col("age") <= 60))
)

print(df_refined.count())
df_refined.limit(3).display()

# COMMAND ----------

# Step 3: Save the processed data back to the catalog as a new table
df_refined.write.format("delta").mode("overwrite").saveAsTable(table_refined_data)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create data to forecasting

# COMMAND ----------

# Step 1: Calculate percentage of payments on time grouped by fecha_corte
df_payments_summary = (
    df_refined.groupBy("fecha_corte")
    .agg(
        F.count("*").alias("total_observations"),
        F.sum(F.when(F.col("payment_on_time") == '1', 1).otherwise(0)).alias("on_time_payments")
    )
    .withColumn("percent_on_time", (F.col("on_time_payments") / F.col("total_observations")) * 100)
    .select("fecha_corte", "percent_on_time")  # Keep only the required columns
)


# Step 2: Order data
df_payments_summary = df_payments_summary.orderBy("fecha_corte", ascending=True)

df_payments_summary.display()

# COMMAND ----------

table_forecasting_data = f"{catalog_name}.{schema_name}.forecast_data"
df_payments_summary.write.format("delta").mode("overwrite").saveAsTable(table_forecasting_data)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Preprocess data to clasificate

# COMMAND ----------

# Parameters
catalog_name = "dbdemos"
schema_name = "demo_payment_default"
table_raw_data = f"{catalog_name}.{schema_name}.data_to_classificate"
table_refined_data = f"{catalog_name}.{schema_name}.refined_data_to_classificate"

# COMMAND ----------

# Step 1: Read data from the raw table
df_raw = spark.read.table(table_raw_data)
print(df_raw.count())
df_raw.limit(3).display()

# COMMAND ----------

# Step 2: Format data types, filtering data and columns
df_refined = (
    df_raw
    .withColumn("monthly_income", col("monthly_income").cast("double"))
    .withColumn("age", col("age").cast("int"))
    .withColumn("employment_years", col("employment_years").cast("int"))
    .withColumn("loan_amount", col("loan_amount").cast("double"))
    .withColumn("credit_score", col("credit_score").cast("int"))
    .withColumn("payment_on_time", col("payment_on_time").cast("int"))
    .select("fecha_corte", "monthly_income", "age", "employment_years", "loan_amount", "credit_score")
    .filter((col("age") >= 18) & (col("age") <= 60))
)

print(df_refined.count())
df_refined.limit(3).display()

# COMMAND ----------

df_refined.write.format("delta").mode("overwrite").saveAsTable(table_refined_data)
