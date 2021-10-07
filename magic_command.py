# Databricks notebook source
display()

# COMMAND ----------

import os

if os.getenv("LOCAL"):
    # NOTE(hori-ryota): to ignore type check
    dbutils: Any = {}

# COMMAND ----------

# MAGIC %python
# MAGIC dbutils.run("test", 10)
