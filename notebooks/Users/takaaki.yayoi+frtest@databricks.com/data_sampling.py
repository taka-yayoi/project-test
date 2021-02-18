# Databricks notebook source
# パス指定 Sparkではdbfs:を使用してアクセス
source_path = 'dbfs:/databricks-datasets/samples/lending_club/parquet/'
delta_path = 'dbfs:/mnt/analytics_data/lending_data/delta/'

dbutils.fs.ls(delta_path)

# COMMAND ----------

# 既存のデータを削除
# Deltaテーブルのパスを削除。
dbutils.fs.rm(delta_path, True)

# ソースディレクトリにあるParquetファイルをデータフレームに読み込む
df = spark.read.parquet(source_path)

# COMMAND ----------

len(df.columns)

# COMMAND ----------

#ここではrandomSplit()を使って、ワークショップでは10%のサンプルを使用して実行
(data, data_rest) = df.randomSplit([0.10, 0.90], seed=123)

# 読み込まれたデータを参照
display(data)

# レコード件数確認
print("全レコード件数:" , df.count())
print("ワークショップで使用するレコード件数:" , data.count())

# 物理パスの位置
print('delta_path:' + delta_path)

# COMMAND ----------

username = "takaakiyayoi"

# COMMAND ----------

# データベースを作成。
spark.sql(f"create database {username}_db")
# データベースを使用。
spark.sql(f"use {username}_db")

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table orijinal_data

# COMMAND ----------

table_name = "original_data"

# COMMAND ----------

# MAGIC %python
# MAGIC # データフレームのデータをdeltaとして登録
# MAGIC df.write.format('delta').mode("overwrite").option("path", delta_path).saveAsTable(table_name)

# COMMAND ----------

df.write.format('delta').mode("append").option("path", delta_path).saveAsTable(table_name)

# COMMAND ----------

display(spark.sql(f"DROP TABLE IF EXISTS {table_name}"))
display(spark.sql(f"CREATE TABLE {table_name} USING DELTA LOCATION '{delta_path}'"))

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM original_data

# COMMAND ----------

df = sql(
  '''
  Select 
  *
  From orijinal_data
  where 1=1
  order by random()
  limit 1000000
  '''
)
display(df)

# COMMAND ----------

df.count()

# COMMAND ----------

