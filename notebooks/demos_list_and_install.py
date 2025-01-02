# Databricks notebook source
# MAGIC %pip install dbdemos

# COMMAND ----------

dbutils.library.restartPython()
import dbdemos
# Listar todas las demos disponibles
demos = dbdemos.list_demos()

# COMMAND ----------

import dbdemos
dbdemos.install('llm-rag-chatbot')

# COMMAND ----------

import dbdemos
dbdemos.install('streaming-sessionization')
