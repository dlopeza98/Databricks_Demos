# Databricks notebook source
# MAGIC %md
# MAGIC # Clasification Risk of No payment

# COMMAND ----------

# Load data to classificate

df_input = spark.read.table ("dbdemos.demo_payment_default.data_to_classificate")
print(df_input.count())
df_input.limit(3).display()

# COMMAND ----------

import mlflow
from pyspark.sql.functions import struct, col
logged_model = 'runs:/ab2b5935909d4090bc73ef66ed3baeba/model'

# Load model as a Spark UDF. Override result_type if the model does not return double values.
loaded_model = mlflow.pyfunc.spark_udf(spark, model_uri=logged_model,env_manager="conda")

# Predict on a Spark DataFrame.
df_output = df_input.withColumn('predictions', loaded_model(struct(*map(col, df_input.columns))))

# COMMAND ----------

print(df_output.count())
df_output.display()
