# Databricks notebook source
# MAGIC %md
# MAGIC # Proyecto: Análisis y Modelado de riesgo crediticio - Compañía XYZ
# MAGIC
# MAGIC Este proyecto tiene como objetivo desarrollar un proceso integral de preprocesamiento y modelado de datos en torno a los datos de créditos de los clientes de la compañía XYZ. La empresa ha identificado que los mayores a 60 años nunca se retrasan en los pagos, por lo tanto, les interesa analizar el riesgo de no pago a tiempo de clientes entre 18 y 60 años
# MAGIC El enfoque principal es realizar análisis predictivos para apoyar las decisiones de negocio relacionadas con el riesgo crediticio y la proyección de tendencias.
# MAGIC
# MAGIC ## Objetivos del Proyecto
# MAGIC
# MAGIC 1. **Modelo de Clasificación**: Crear un modelo que permita predecir cuáles clientes tienen alta probabilidad de no pagar a tiempo en el mes en curso.
# MAGIC 2. **Proyección Financiera**: Desarrollar un análisis que proyecte el valor de los créditos y el porcentaje de clientes que no cumplen con sus pagos a tiempo en cada mes.
# MAGIC
# MAGIC ## Estructura del Proyecto
# MAGIC
# MAGIC El proyecto está organizado en las siguientes carpetas principales:
# MAGIC
# MAGIC ### 1. **Setup**
# MAGIC    - Contiene un notebook encargado de generar los datos históricos de manera aleatoria para simular el comportamiento crediticio de los clientes de XYZ. Este notebook crea datos representativos para análisis y modelado.
# MAGIC
# MAGIC ### 2. **Data_Processing**
# MAGIC    - Incluye un notebook que realiza el preprocesamiento del *raw data*. Este proceso incluye limpieza de datos, transformación de variables y creación de nuevas columnas necesarias para los modelos predictivos.
# MAGIC
# MAGIC ### 3. **Models**
# MAGIC    - **Clasificación**: Contiene un notebook dedicado al desarrollo y entrenamiento de un modelo de clasificación para identificar clientes con alta probabilidad de incumplimiento.
# MAGIC    - **Predicción**: Incluye dos notebooks:
# MAGIC      - Uno para proyectar el valor total de los créditos en el tiempo.
# MAGIC      - Otro para predecir el porcentaje mensual de clientes que no pagan a tiempo.
# MAGIC
# MAGIC ### 4. App
# MAGIC    - Se requiere una App con Shiny para que muestre los resultados de los modelos predictivos. Esta App debe estar disponible para el consumo de los ususario del negocio 24/7 de manera segura.
# MAGIC
# MAGIC ## Resultados y Consumo de Datos
# MAGIC
# MAGIC - Los resultados generados por los modelos se guardan en el **Unity Catalog** de Databricks. 
# MAGIC - Estos resultados están diseñados para ser fácilmente consumidos y analizados por las aplicaciones del negocio, ofreciendo insights valiosos que soportan la toma de decisiones estratégicas.
# MAGIC
# MAGIC ## Uso del Proyecto
# MAGIC
# MAGIC 1. Ejecuta el notebook en la carpeta `Setup` para generar los datos históricos simulados.
# MAGIC 2. Realiza el preprocesamiento utilizando el notebook en `Data_Processing`.
# MAGIC 3. Entrena y evalúa los modelos predictivos utilizando los notebooks en la carpeta `Models`.
# MAGIC
# MAGIC ## Nota Importante
# MAGIC
# MAGIC En un escenario productivo, el paso de creación de datos aleatorios sería reemplazado por un proceso de **ingesta de datos reales del negocio**, asegurando que los análisis y modelos se construyan sobre información representativa y actualizada.
# MAGIC
