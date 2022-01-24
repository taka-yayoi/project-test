# Databricks notebook source
# MAGIC %md
# MAGIC # 全てを再現可能に：機械学習とデータレイクハウスの出会い
# MAGIC 
# MAGIC 本ノートブックでは、Databricksにおけるプロジェクト資産の管理方法をご説明します。データの読み込み、機械学習モデルのトレーニングを行い、その際に用いられたノートブック、機械学習モデル、パラメーター、データのバージョンをMLflowに記録します。
# MAGIC <br><br>
# MAGIC - **ノートブック**： Reposを用いたバージョン管理、共同作業
# MAGIC - **機械学習モデル**： MLflowのトラッキング、モデルレジストリによる集中管理
# MAGIC - **データ**： Delta Lakeのバージョン管理機能の活用
# MAGIC - **その他のアーティファクト**： MLflowのアーティファクト管理を活用
# MAGIC 
# MAGIC <table>
# MAGIC   <tr><th>作者</th><th>Databricks Japan</th></tr>
# MAGIC   <tr><td>日付</td><td>2021/07/10</td></tr>
# MAGIC   <tr><td>バージョン</td><td>1.0</td></tr>
# MAGIC   <tr><td>クラスター</td><td>8.3ML</td></tr>
# MAGIC </table>
# MAGIC 
# MAGIC <img style="margin-top:25px;" src="https://sajpstorage.blob.core.windows.net/workshop20210205/databricks-logo-small-new.png" width="140">
# MAGIC 
# MAGIC **参考資料**
# MAGIC - [全てを再現可能に：機械学習とデータレイクハウスの出会い \- Qiita](https://qiita.com/taka_yayoi/items/cb1fafc96e7337d1fa58)

# COMMAND ----------

import mlflow
import mlflow.sklearn
from sklearn.metrics import mean_squared_error, mean_absolute_error, r2_score
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestRegressor
import pyspark.sql.functions as F
from delta.tables import *
import numpy as np

# COMMAND ----------

# MAGIC %md
# MAGIC ## DBFSからワインデータを読み込み

# COMMAND ----------

# データの読み込み 
path = '/databricks-datasets/wine-quality/winequality-white.csv'
wine_df = (spark.read
           .option('header', 'true')
           .option('inferSchema', 'true')
           .option('sep', ';')
           .csv(path))
wine_df_clean = wine_df.select([F.col(col).alias(col.replace(' ', '_')) for col in wine_df.columns])
display(wine_df_clean)

# COMMAND ----------

# MAGIC %md
# MAGIC ## ディレクトリの作成
# MAGIC 
# MAGIC 機械学習モデルのトレーニングで用いるデータをDelta Lake形式で保存することで、データのバージョン管理を可能にします。

# COMMAND ----------

# MAGIC %fs mkdirs /tmp/takaakiyayoidatabrickscom/reproducible_ml_blog

# COMMAND ----------

# MAGIC %md
# MAGIC ## Deltaとしてデータを書き出し

# COMMAND ----------

# Deltaテーブルにデータを書き出し 
write_path = 'dbfs:/tmp/takaakiyayoidatabrickscom/reproducible_ml_blog/wine_quality_white.delta'
wine_df_clean.write.format('delta').mode('overwrite').save(write_path)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 新規行の追加

# COMMAND ----------

new_row = spark.createDataFrame([[7, 0.27, 0.36, 1.6, 0.045, 45, 170, 1.001, 3, 0.45, 8.8, 6]])
wine_df_extra_row = wine_df_clean.union(new_row)
display(wine_df_extra_row)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Deltaテーブルの上書き、スキーマの更新
# MAGIC 
# MAGIC Delta Lake形式のテーブルに対する更新処理は全て記録されます。

# COMMAND ----------

# Deltaの格納場所に上書き 
wine_df_extra_row.write.format('delta').mode('overwrite').option('overwriteSchema', 'true').save(write_path)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Deltaのテーブル履歴
# MAGIC 
# MAGIC テーブル作成時はバージョン0ですが、上記更新処理でバージョン1が作成されていることを確認できます。以下の例ではPython APIを使用していますが、`DESCRIBE HISTORY <table name>`のように、SQLでもバージョン履歴にアクセスすることができます。
# MAGIC 
# MAGIC 参考資料:
# MAGIC - [Delta Lakeにおけるテーブルユーティリティコマンド \- Qiita](https://qiita.com/taka_yayoi/items/152a31dc44bda51eeecd)

# COMMAND ----------

from delta.tables import *

deltaTable = DeltaTable.forPath(spark, write_path)
fullHistoryDF = deltaTable.history()    # バージョンを選択するためにテーブルの完全な履歴を取得

display(fullHistoryDF)

# COMMAND ----------

# MAGIC %md
# MAGIC ## トレーニングのためのデータのバージョンを指定
# MAGIC 
# MAGIC 明示的にデータバージョンを指定してモデルのトレーニングを行うことができます。また、このデータバージョンを記録しておくことで、実験を容易に再現することが可能となります。

# COMMAND ----------

# モデルトレーニングのためのデータバージョンを指定
version = 1 
wine_df_delta = spark.read.format('delta').option('versionAsOf', version).load(write_path).toPandas()
display(wine_df_delta)

# COMMAND ----------

# MAGIC %md
# MAGIC ## データの分割

# COMMAND ----------

# データをトレーニングデータセットとテストデータセットに分割 (0.75, 0.25)の割合で分割
seed = 1111
train, test = train_test_split(wine_df_delta, train_size=0.75, random_state=seed)

# スカラー[3, 9]の値を取る目標変数"quality"カラム 
X_train = train.drop(['quality'], axis=1)
X_test = test.drop(['quality'], axis=1)
y_train = train[['quality']]
y_test = test[['quality']]

# COMMAND ----------

# MAGIC %md
# MAGIC ## MLflowを用いてモデルを構築
# MAGIC 
# MAGIC Databricksは[MLflow](https://mlflow.org/)を用いて、自動的に全ての**ラン(トレーニング)**を追跡します。MLflowのUIを用いて、構築したモデルを比較することができます。下のセルの実行後、画面右上にある**Experiment**ボタンを押してみてください。
# MAGIC 
# MAGIC <a href="https://www.mlflow.org/docs/latest/index.html"><img width=100 src="https://www.mlflow.org/docs/latest/_static/MLflow-logo-final-black.png" title="MLflow Documentation — MLflow 1.15.0 documentation"></a>
# MAGIC 
# MAGIC 参考情報：
# MAGIC - [PythonによるDatabricks MLflowクイックスタートガイド \- Qiita](https://qiita.com/taka_yayoi/items/dd81ac0da656bf883a34)
# MAGIC 
# MAGIC MLflowのトラッキングを用いることで、機械学習モデルの本体、ハイパーパラメータ、精度指標などをまとめて記録することができます。以下の例では、ハイパーパラメータに以下の追加情報も含めています。要件に合わせて必要な情報を機械学習モデルに紐づけることができます。
# MAGIC <br><br>
# MAGIC - Delta Lakeのデータバージョン
# MAGIC - アルコールと密度の散布図のグラフ
# MAGIC 
# MAGIC ログの取得方法は以下の通りとなります。
# MAGIC <head>
# MAGIC   <link href="https://fonts.googleapis.com/css2?family=Kosugi+Maru&display=swap" rel="stylesheet">
# MAGIC   <style>
# MAGIC     h1,h2,h3,p,span,td, div {font-family: "Kosugi Maru", sans-serif !important;}
# MAGIC   </style>
# MAGIC </head>
# MAGIC 1. まず最初に [mlflow.start_run()](https://www.mlflow.org/docs/latest/python_api/mlflow.html#mlflow.start_run) を実行<br> 
# MAGIC 1. ブロック内でモデル学習を実行<br>
# MAGIC 1. モデルをロギングするために [mlflow.spark.log_model()](https://www.mlflow.org/docs/latest/python_api/mlflow.spark.html#mlflow.spark.log_model) を実行<br> 
# MAGIC 1. ハイパーパラメーターをロギングするために [mlflow.log_param()](https://www.mlflow.org/docs/latest/python_api/mlflow.html#mlflow.log_param) を実行<br> 
# MAGIC 1. モデルメトリックスをロギングするために [mlflow.log_metric()](https://www.mlflow.org/docs/latest/python_api/mlflow.html#mlflow.log_metric) を実行<br>
# MAGIC 
# MAGIC きめ細かい指定をするためには上のメソッドを使用しますが、デフォルトのパラメーター、メトリクスをロギングするので十分であれば、`mlflow.spark.autolog`によるオートロギングを利用できます。
# MAGIC 
# MAGIC [mlflow.spark.autolog](https://www.mlflow.org/docs/latest/python_api/mlflow.spark.html#mlflow.spark.autolog)

# COMMAND ----------

# アーティファクトとして登録するグラフ用
import matplotlib.pyplot as plt

with mlflow.start_run() as run:
  # パラメーターのロギング
  n_estimators = 1000
  max_features = 'sqrt'
  params = {'data_version': version,
           'n_estimators': n_estimators,
           'max_features': max_features}
  mlflow.log_params(params)
  # モデルのトレーニング 
  rf = RandomForestRegressor(n_estimators=n_estimators, max_features=max_features, random_state=seed)
  rf.fit(X_train, y_train)

  # テストデータを用いた予測
  preds = rf.predict(X_test)

  # メトリクスの生成 
  rmse = np.sqrt(mean_squared_error(y_test, preds))
  mae = mean_absolute_error(y_test, preds)
  r2 = r2_score(y_test, preds)
  metrics = {'rmse': rmse,
             'mae': mae,
             'r2' : r2}

  # メトリクスのロギング
  mlflow.log_metrics(metrics)

  # モデルのロギング 
  mlflow.sklearn.log_model(rf, 'model')  
  
  # アーティファクト（任意のバイナリなど、グラフなども記録可能）のロギング
  # 記録したいアーティファクトのパスをmlflow.log_artifactに指定します
  plt.clf()
  X_train.plot(kind="scatter", x="alcohol", y="density")
  figPath = "/tmp/alcohol-density.png" # local path
  plt.savefig(figPath)
  mlflow.log_artifact(figPath)
  plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## モデルレジストリによるモデルの集中管理
# MAGIC 
# MAGIC この時点ではモデルは**エクスペリメント(実験)**として記録されている状態です。例えば、このモデルを本格運用に移行したい、APIを経由して利用したい、という場合には**モデルレジストリ**に登録を行います。
# MAGIC 
# MAGIC モデルレジストリにモデルを登録することで、以下の管理を行うことができます。
# MAGIC 
# MAGIC - **モデルバージョン**: モデルレジストリに新規モデルが登録されると、バージョン1として追加されます。同じモデル名に登録されるそれぞれのモデルは、バージョン番号をインクリメントします。
# MAGIC - **モデルステージ**: モデルバージョンは1つ以上のステージに割り当てることができます。MLflowは一般的なユースケースに対応する**None**、**Staging**、**Production**、**Archived**のステージを事前に定義しています。適切なアクセス権を持っていれば、モデルバージョンをステージ間で遷移させるか、遷移をリクエストすることができます。
# MAGIC - **説明文**: アルゴリズムの説明、使用したデータセット、方法論などチームにとって有用な情報、説明を含むモデルの意図を注釈することができます。
# MAGIC - **アクティビティ**: 登録されたそれぞれのモデルに対する、ステージ遷移のリクエストのようなアクティビティは記録されます。アクティビティの追跡によって、実験段階からステージング、プロダクションに至るモデルの進化におけるリネージュや監査可能性を提供します。
# MAGIC 
# MAGIC 参考情報：
# MAGIC - [DatabricksにおけるMLflowモデルレジストリ \- Qiita](https://qiita.com/taka_yayoi/items/e7a4bec6420eb7069995)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Reposによるコードに対する共同作業
# MAGIC 
# MAGIC Databricksのノートブックはデフォルトでもバージョン管理が行われますが、チームでの開発ではコード管理システムとの連携が必須となります。この場合、DatabricksのReposを活用することで、GitHubなどのバージョン管理システムとノートブックを同期させることができます。
# MAGIC 
# MAGIC 参考資料:
# MAGIC - [Databricks ReposによるGit連携 \- Qiita](https://qiita.com/taka_yayoi/items/b89f199ff0d3a4c16140)

# COMMAND ----------

# MAGIC %md
# MAGIC ## クリーンアップ

# COMMAND ----------

dbutils.fs.rm("/tmp/takaakiyayoidatabrickscom/reproducible_ml_blog", True)

# COMMAND ----------

# MAGIC %md
# MAGIC # END
