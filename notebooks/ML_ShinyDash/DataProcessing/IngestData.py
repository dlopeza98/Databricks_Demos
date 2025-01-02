# Databricks notebook source
# MAGIC %md
# MAGIC # Data Creation
# MAGIC
# MAGIC This notebook processes credit payment data stored in `/Volumes/dbdemos/demo_payment_default/source_data` and organizes it into two categories:
# MAGIC
# MAGIC 1. **Raw Data**: Includes all records where the `fecha_corte` (cut-off date) is earlier than the current month and year of execution. This data is stored in the `raw_data` table in the catalog `dbdemos`, schema `demo_payment_default`. If the table does not exist, it is created. New data is appended to the table.
# MAGIC
# MAGIC 2. **Data to Classify**: Includes records where:
# MAGIC    - `fecha_corte` is within the current month and year of execution, and
# MAGIC    - `fecha_pago` (payment date) is later than the execution date.
# MAGIC    This data is stored in the `data_to_classificate` table in the same schema. The content of this table is truncated before adding the new data to ensure only the latest records are available.
# MAGIC
# MAGIC ## Workflow
# MAGIC
# MAGIC 1. **Data Ingestion**: 
# MAGIC    - All CSV files from the specified path are loaded into a Spark DataFrame.
# MAGIC
# MAGIC 2. **Data Filtering**: 
# MAGIC    - Two DataFrames are created based on the `fecha_corte` and `fecha_pago` criteria: `df_raw` and `df_to_predict`.
# MAGIC
# MAGIC 3. **Table Management**:
# MAGIC    - The `raw_data` table is updated by appending the new records.
# MAGIC    - The `data_to_classificate` table is updated by overwriting the previous content.
# MAGIC
# MAGIC ## Output
# MAGIC
# MAGIC The processed data is stored in the Unity Catalog under:
# MAGIC - **`dbdemos.demo_payment_default.raw_data`**
# MAGIC - **`dbdemos.demo_payment_default.data_to_classificate`**
# MAGIC
# MAGIC This process ensures the data is clean, organized, and ready for further analysis or modeling tasks.
# MAGIC

# COMMAND ----------

import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, month
from datetime import datetime


# Ruta de origen
source_path = "/Volumes/dbdemos/demo_payment_default/source_data/"

# Cargar todos los archivos CSV desde la ruta de origen
df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(source_path)

# Obtener la fecha actual
execution_date = datetime.now()

# Convertir las columnas de fecha a tipo de dato de Spark
df = df.withColumn("fecha_corte", col("fecha_corte").cast("date"))
df = df.withColumn("fecha_pago", col("fecha_pago").cast("date"))

# Filtrar los datos para df_raw
df_raw = df.filter(col("fecha_corte") < execution_date.replace(day=1))

# Filtrar los datos para df_to_predict
df_to_predict = df.filter(
    (year(col("fecha_corte")) == execution_date.year) &
    (month(col("fecha_corte")) == execution_date.month) &
    (col("fecha_pago") > execution_date)
)

# COMMAND ----------

df_raw.display()

# COMMAND ----------

df_to_predict.display()

# COMMAND ----------

catalog_name = "dbdemos"
schema_name = "demo_payment_default"
table_raw_data = f"{catalog_name}.{schema_name}.raw_data"
table_to_predict = f"{catalog_name}.{schema_name}.data_to_classificate"

# Crear las tablas si no existen y agregar los datos
# Guardar df_raw en la tabla "raw_data" (crear si no existe)
df_raw.write.format("delta").mode("overwrite").saveAsTable(table_raw_data)

# Guardar df_to_predict en la tabla "data_to_classificate" (truncar antes de guardar)
df_to_predict.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(table_to_predict)

print(f"Datos procesados y guardados en las tablas {table_raw_data} y {table_to_predict}.")

