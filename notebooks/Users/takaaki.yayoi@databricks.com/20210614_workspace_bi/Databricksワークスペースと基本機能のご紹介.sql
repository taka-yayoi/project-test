-- Databricks notebook source
-- MAGIC %md # Databricksのご紹介
-- MAGIC 
-- MAGIC **レイクハウスプラットフォーム**
-- MAGIC ![](https://sajpstorage.blob.core.windows.net/demo20210614-18/lakehouse.png)
-- MAGIC 
-- MAGIC **Delta Lake**
-- MAGIC ![](https://sajpstorage.blob.core.windows.net/demo20210614-18/deltalake.png)
-- MAGIC 
-- MAGIC **アーキテクチャ**
-- MAGIC ![](https://sajpstorage.blob.core.windows.net/demo20210614-18/architecture.png)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Databricksで実践するData &amp; AI
-- MAGIC <head>
-- MAGIC   <link href="https://fonts.googleapis.com/css2?family=Kosugi+Maru&display=swap" rel="stylesheet">
-- MAGIC   <style>
-- MAGIC     h1,h2,h3,p,span,td, div {font-family: "Kosugi Maru", sans-serif !important;}
-- MAGIC   </style>
-- MAGIC </head>
-- MAGIC 
-- MAGIC 本デモでは、Databricksの基本的な使い方を、探索的データ分析(EDA:Exploratory Data Analysis)の流れに沿ってご説明します。また、データレイクに高パフォーマンス、高信頼性をもたらすDelta Lakeをご紹介します。
-- MAGIC <table>
-- MAGIC   <tr><th>作者</th><th>Databricks Japan</th></tr>
-- MAGIC   <tr><td>日付</td><td>2021/06/03</td></tr>
-- MAGIC   <tr><td>バージョン</td><td>1.4</td></tr>
-- MAGIC   <tr><td>クラスター</td><td>8.3ML</td></tr>
-- MAGIC </table>
-- MAGIC <img style="margin-top:25px;" src="https://jixjiadatabricks.blob.core.windows.net/images/databricks-logo-small-new.png" width="140">

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 基本的な使い方
-- MAGIC 
-- MAGIC 画面左の**サイドバー**からDatabricksの主要な機能にアクセスします。
-- MAGIC 
-- MAGIC サイドバーのコンテンツは選択するペルソナ(**Data Science & Engineering**、**Machine Learning**、**SQL**)によって決まります。
-- MAGIC 
-- MAGIC - **Data Science & Engineering:** PythonやR、SQLを用いてノートブックを作成し実行するペルソナ
-- MAGIC - **Machine Learning:** モデル管理や特徴量ストアを活用してノートブックを作成するペルソナ
-- MAGIC - **SQL:** BIを行うペルソナ

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### サイドバーの使い方
-- MAGIC 
-- MAGIC - デフォルトではサイドバーは畳み込まれた状態で表示され、アイコンのみが表示されます。サイドバー上にカーソルを移動すると全体を表示することができます。
-- MAGIC - ペルソナを変更するには、Databricksロゴの直下にあるアイコンからペルソナを選択します。
-- MAGIC 
-- MAGIC ![](https://docs.databricks.com/_images/change-persona.gif)
-- MAGIC 
-- MAGIC - 次回ログイン時に表示されるペルソナを固定するには、ペルソナの隣にある![](https://docs.databricks.com/_images/persona-pin.png)をクリックします。再度クリックするとピンを削除することができます。
-- MAGIC - サイドバーの一番下にある**Menu options**で、サイドバーの表示モードを切り替えることができます。Auto(デフォルト)、Expand(展開)、Collapse(畳み込み)から選択できます。
-- MAGIC - 機械学習に関連するページを開く際には、ペルソナは自動的に**Machine Learning**に切り替わります。

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Databricksのヘルプリソース
-- MAGIC 
-- MAGIC Databricksには、Apache SparkとDatabricksを効果的に使うために学習する際に、助けとなる様々なツールが含まれています。Databricksには膨大なApache Sparkのドキュメントが含まれており。Webのどこからでも利用可能です。リソースには大きく二つの種類があります。Apace SparkとDatabricksの使い方を学ぶためのものと、基本を理解した方が参照するためのリソースです。
-- MAGIC 
-- MAGIC これらのリソースにアクセスするには、画面右上にあるクエスチョンマークをクリックします。サーチメニューでは、以下のドキュメントを検索することができます。
-- MAGIC 
-- MAGIC ![img](https://sajpstorage.blob.core.windows.net/demo20210421-spark-introduction/help_menu.png)
-- MAGIC 
-- MAGIC - **Help Center(ヘルプセンター)**
-- MAGIC   - [Help Center \- Databricks](https://help.databricks.com/s/)にアクセスして、ドキュメント、ナレッジベース、トレーニングなどのリソースにアクセスできます。
-- MAGIC - **Release Notes(リリースノート)**
-- MAGIC   - 定期的に実施される機能アップデートの内容を確認できます。
-- MAGIC - **Documentation(ドキュメント)**
-- MAGIC   - マニュアルにアクセスできます。
-- MAGIC - **Knowledge Base(ナレッジベース)**
-- MAGIC   - 様々なノウハウが蓄積されているナレッジベースにアクセスできます。
-- MAGIC - **Feedback(フィードバック)**
-- MAGIC   - 製品に対するフィードバックを投稿できます。
-- MAGIC - **Shortcuts(ショートカット)**
-- MAGIC   - キーボードショートカットを表示します。
-- MAGIC     
-- MAGIC また、Databricksを使い始める方向けに資料をまとめたDatabricksクイックスタートガイドもご活用ください。<br><br>
-- MAGIC 
-- MAGIC - [Databricksクイックスタートガイド \- Qiita](https://qiita.com/taka_yayoi/items/125231c126a602693610)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### クラスターを作成
-- MAGIC <head>
-- MAGIC   <link href="https://fonts.googleapis.com/css2?family=Kosugi+Maru&display=swap" rel="stylesheet">
-- MAGIC   <style>
-- MAGIC     h1,h2,h3,p,span,td, div {font-family: "Kosugi Maru", sans-serif !important;}
-- MAGIC   </style>
-- MAGIC </head>
-- MAGIC 
-- MAGIC <div style='line-height:1.5rem; padding-top: 10px;'>
-- MAGIC (1) 左のサイドバーの **Clusters** を右クリック。新しいタブもしくはウィンドウを開く。<br>
-- MAGIC (2) クラスタページにおいて **Create Cluster** をクリック。<br>
-- MAGIC (3) クラスター名を **[自分の名前(takaakiなど)]** とする。<br>
-- MAGIC (4) Databricks ランタイム バージョン を ドロップダウンし、 例えば **7.4ML (Scala 2.12, Spark 3.0.1)** を選択。<br>
-- MAGIC (5) 最後に **Create Cluster** をクリックすると、クラスターが起動 !
-- MAGIC </div>
-- MAGIC 
-- MAGIC ![](https://sajpstorage.blob.core.windows.net/demo20210603-workspace/create_cluster.png)
-- MAGIC 
-- MAGIC ### ノートブックを作成したクラスターに紐付けて、 run all コマンドを実行
-- MAGIC <head>
-- MAGIC   <link href="https://fonts.googleapis.com/css2?family=Kosugi+Maru&display=swap" rel="stylesheet">
-- MAGIC   <style>
-- MAGIC     h1,h2,h3,p,span,td, div {font-family: "Kosugi Maru", sans-serif !important;}
-- MAGIC   </style>
-- MAGIC </head>
-- MAGIC 
-- MAGIC <div style='line-height:1.5rem; padding-top: 10px;'>
-- MAGIC (1) ノートブックに戻ります。<br>
-- MAGIC (2) 左上の ノートブック メニューバーから、 **<img src="http://docs.databricks.com/_static/images/notebooks/detached.png"/></a> > [自分の名前]** を選択。<br>
-- MAGIC (3) クラスターが <img src="http://docs.databricks.com/_static/images/clusters/cluster-starting.png"/></a> から <img src="http://docs.databricks.com/_static/images/clusters/cluster-running.png"/></a> へ変更となったら  **<img src="http://docs.databricks.com/_static/images/notebooks/run-all.png"/></a> Run All** をクリック。<br>
-- MAGIC </div>
-- MAGIC 
-- MAGIC 本デモでは操作の流れが分かるようにステップ毎に実行していきます。

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## データの操作

-- COMMAND ----------

-- MAGIC %md ### 準備：データベース、ファイルパスの設定
-- MAGIC 
-- MAGIC ノートブック名の右に`(SQL)`と表示されているのは、このノートブックのデフォルト言語がSQLであることを意味しています。クリックすることでデフォルト言語を変更することができます。ノートブックレベルでの言語指定に加えて、下のセルにあるようにセルレベルでの言語指定が可能です。`%python`というマジックワードを指定することで、当該セルの言語をPythonにすることができます。<br><br>
-- MAGIC 
-- MAGIC - [Databricksノートブックを使う \- Qiita](https://qiita.com/taka_yayoi/items/dfb53f63aed2fbd344fc#%E6%B7%B7%E6%88%90%E8%A8%80%E8%AA%9E)
-- MAGIC 
-- MAGIC DatabricksにはHiveメタストアが同梱されています。メタストアにデータを登録することで、SQLによるアクセスが可能になります。ファイルはDatabricks File System(DBFS)上に格納されます。DBFSの実態はS3です。<br><br>
-- MAGIC 
-- MAGIC - [Databricksにおけるデータベースおよびテーブル \- Qiita](https://qiita.com/taka_yayoi/items/e7f6982dfbee7fc84894)
-- MAGIC - [Databricksにおけるファイルシステム \- Qiita](https://qiita.com/taka_yayoi/items/e16c7272a7feb5ec9a92)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC import re
-- MAGIC from pyspark.sql.types import * 
-- MAGIC 
-- MAGIC # ログインIDからUsernameを取得
-- MAGIC username_raw = dbutils.notebook.entry_point.getDbutils().notebook().getContext().tags().apply('user')
-- MAGIC # Username の英数字以外を除去し、全て小文字化。Username をファイルパスやデータベース名の一部で使用可能にするため。
-- MAGIC username = re.sub('[^A-Za-z0-9]+', '', username_raw).lower()
-- MAGIC 
-- MAGIC # データベース名
-- MAGIC db_name = f"20210617_workshop_{username}"
-- MAGIC 
-- MAGIC # Hiveメタストアのデータベースの準備:データベースの作成
-- MAGIC spark.sql(f"CREATE DATABASE IF NOT EXISTS {db_name}")
-- MAGIC # Hiveメタストアのデータベースの選択
-- MAGIC spark.sql(f"USE {db_name}")
-- MAGIC 
-- MAGIC # ファイル格納パス
-- MAGIC work_path = f"dbfs:/tmp/20210617_workshop/{username}/"
-- MAGIC 
-- MAGIC print("database name: " + db_name)
-- MAGIC print("path: " + work_path)

-- COMMAND ----------

-- MAGIC %md ### 準備：作成済みテーブルの削除
-- MAGIC 
-- MAGIC 以下のセルではデモで使用するテーブルの初期化を行なっています。ノートブックのデフォルト言語がSQLなので、以下のセルはSQLと解釈されます。

-- COMMAND ----------

DROP TABLE IF EXISTS diamonds;
DROP TABLE IF EXISTS property_data_kanto;
DROP TABLE IF EXISTS property_kanto_delta;
DROP TABLE IF EXISTS property_summary_delta;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Databricksデータセットからテーブルを作成する：CSVファイル
-- MAGIC 
-- MAGIC Databricks環境の`/databricks-datasets`配下には様々なサンプルデータが格納されています。定期的に更新されるCOVID-19データセットも含まれています。
-- MAGIC 
-- MAGIC [DatabricksにおけるCOVID\-19データセットの活用: データコミュニティで何ができるのか \- Qiita](https://qiita.com/taka_yayoi/items/3d62c4dbdc0e39e4772c)

-- COMMAND ----------

-- テーブルが存在する場合には削除します
DROP TABLE IF EXISTS diamonds;

-- OPTIONSでcsvのファイルパス(path)、ヘッダーがあること(header)を指定し、USINGでファイルフォーマットを指定してdiamondsテーブを作成します
CREATE TABLE diamonds
USING csv
OPTIONS (path "/databricks-datasets/Rdatasets/data-001/csv/ggplot2/diamonds.csv", header "true")

-- COMMAND ----------

-- 従来のデータベース同様にテーブルへの問い合わせを行うことができます
SELECT * from diamonds

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Blobからデータを取得する: Parquetファイル
-- MAGIC <head>
-- MAGIC   <link href="https://fonts.googleapis.com/css2?family=Kosugi+Maru&display=swap" rel="stylesheet">
-- MAGIC   <style>
-- MAGIC     * {font-family: "Kosugi Maru", sans-serif !important;}
-- MAGIC   </style>
-- MAGIC </head>
-- MAGIC [Apache Parquetとは](https://databricks.com/jp/glossary/what-is-parquet)
-- MAGIC > Parquetとは、Hadoopエコシステムの各種プロジェクトで利用可能なオープンソースのファイルフォーマットです。Apache Parquetは、CSVやTSVファイルのような行指向ファイル形式に対し、効率的で高性能な列指向ストレージ形式です。
-- MAGIC 
-- MAGIC ここではPythonを用いて関東地方の物件データを読み込みます。PySparkを使用してSparkにアクセスします。
-- MAGIC 
-- MAGIC - [Databricks Apache Sparkクイックスタート \- Qiita](https://qiita.com/taka_yayoi/items/bf5fb09a0108aa14770b)
-- MAGIC - [Databricksにおけるデータのインポート、読み込み、変更 \- Qiita](https://qiita.com/taka_yayoi/items/4fa98b343a91b8eaa480)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC 
-- MAGIC # 公開Azure Storage Blobから学習データを取得します (WASBプロトコル)
-- MAGIC sourcePath = 'wasbs://mokmok@jixjiadatabricks.blob.core.windows.net/property_data_kanto/'
-- MAGIC 
-- MAGIC # PySpark APIを使用してSparkデータフレームを取得します
-- MAGIC # format:読み取るファイルのフォーマットを指定します
-- MAGIC # load:ファイルパス
-- MAGIC # cache:データフレームをメモリーにキャッシュ
-- MAGIC df = spark.read\
-- MAGIC           .format('parquet')\
-- MAGIC           .load(sourcePath)\
-- MAGIC           .cache()   #イテレーティブに使うのでメモリーにキャッシュする
-- MAGIC 
-- MAGIC # テーブルとして登録 (Hiveマネージドテーブル)
-- MAGIC df.write.mode('overwrite').saveAsTable('property_data_kanto')
-- MAGIC 
-- MAGIC # display関数を使用することで、データフレームの内容を並び替えたり、グラフで可視化を行うことができます。
-- MAGIC display(df)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Sparkデータフレームでもpandasと同様のメソッド(一部)を利用できます。
-- MAGIC df.count()

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Delta形式にする：高い信頼性と高性能なデータレイクへ
-- MAGIC 
-- MAGIC <img src="https://jixjiadatabricks.blob.core.windows.net/images/delta-lake-square-black.jpg" width="220">
-- MAGIC 
-- MAGIC **Delta Lakeの特徴**
-- MAGIC 
-- MAGIC 1. ACIDトランザクションによるデータの信頼性確保
-- MAGIC 2. 大容量メタデータに対する高速分散処理
-- MAGIC 3. Time Travel (データのバージョン管理)
-- MAGIC 4. オープンフォーマット (Apache Parquet)
-- MAGIC 5. バッチとストリーミング両方に対応
-- MAGIC 6. スキーマ強制によるデータ品質の担保
-- MAGIC 7. 監査ログによるデータ捜査の追跡
-- MAGIC 8. データの更新・削除
-- MAGIC 9. Apache Spark API準拠
-- MAGIC 
-- MAGIC [Delta Lakeクイックスタートガイド \- Qiita](https://qiita.com/taka_yayoi/items/345f503d5f8177084f24)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # デルタレイクにおけるフォルダを指定
-- MAGIC DELTALAKE_BRONZE_PATH = f"{work_path}/delta/property_kanto"
-- MAGIC 
-- MAGIC # 既にフォルダがある場合は削除
-- MAGIC dbutils.fs.rm(DELTALAKE_BRONZE_PATH, recurse=True)
-- MAGIC 
-- MAGIC # Delta形式で書き込みます。formatに"delta"を指定します。
-- MAGIC df.write.format("delta").save(DELTALAKE_BRONZE_PATH)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### パスを指定して参照もできるが・・・

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # spark.sqlを使用することでSQLを記述できます
-- MAGIC display(spark.sql("select * from delta.`{}` order by 2 desc limit 5".format(DELTALAKE_BRONZE_PATH)))

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### パスを指定して参照もできるが、Deltaテーブル化した方がアクセスは簡単

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # SQL文でHiveメタストアにテーブルを作成します
-- MAGIC display(spark.sql("DROP TABLE IF EXISTS property_kanto_delta;"))
-- MAGIC display(spark.sql("CREATE TABLE property_kanto_delta USING DELTA LOCATION '{}';".format(DELTALAKE_BRONZE_PATH)))

-- COMMAND ----------

-- describe detailでテーブルのメタデータを確認できます
describe detail property_kanto_delta

-- COMMAND ----------

-- describe historyでテーブルの更新履歴を表示します
describe history property_kanto_delta

-- COMMAND ----------

-- MAGIC %md-sandbox  <span style="color:green"><strong>テーブルを作成すると、右の [Dataタブ] にも表示されます。</strong></span>

-- COMMAND ----------

SELECT * from property_kanto_delta

-- COMMAND ----------

-- DBTITLE 1,県別・築年ごとに集計します
select prefecture, agebucket, count(*) from property_kanto_delta group by 1,2 order by 3 desc

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 関東物件販売データに対する探索的データ分析(EDA)
-- MAGIC <head>
-- MAGIC   <link href="https://fonts.googleapis.com/css2?family=Kosugi+Maru&display=swap" rel="stylesheet">
-- MAGIC   <style>
-- MAGIC     * {font-family: "Kosugi Maru", sans-serif !important;}
-- MAGIC   </style>
-- MAGIC </head>
-- MAGIC 
-- MAGIC <p>今回の学習データはすでにクレンジングと加工済みですので、そのまま使ってモデルを作成することも可能です。</p>
-- MAGIC <br>
-- MAGIC <h2>特徴量説明</h2>
-- MAGIC <ul>
-- MAGIC   <li>PropertyName: 物件名 or 建物名</li>
-- MAGIC   <li>Price: 売却価格 (単位：万円)</li>
-- MAGIC   <li>Prefecture: 物件所在都道府県</li>
-- MAGIC   <li>Municipality: 物件市区町村</li>
-- MAGIC   <li>Address: 物件住所 (プライバシー保護の為番地以降は省略)</li>
-- MAGIC   <li>FloorPlan: 間取り (例：1LDK, 3LDK+S)</li>
-- MAGIC   <li>PublicTransportName: 最寄り駅路線名 (例：JR中央線、都営三田線)</li>
-- MAGIC   <li>NearestStation: 最寄り駅名</li>
-- MAGIC   <li>DistanceMethod: 最寄り駅へのアクセス方法 (例：徒歩、バス、車)</li>
-- MAGIC   <li>Distance_min: 最寄り駅へのアクセス時間 (単位：分)</li>
-- MAGIC   <li>FloorSpace_m2: 専有面積 (単位：平米)</li>
-- MAGIC   <li>OtherSpace_m2: バルコニー面積 (単位：平米)</li>
-- MAGIC   <li>BuiltYear: 築年 (例：1998、2018)</li>
-- MAGIC </ul>
-- MAGIC <span style='color:green;'>
-- MAGIC 人工的に作られた特徴量：
-- MAGIC <ul>
-- MAGIC   <li>Age: 築年数 (BuiltYearから算出)</li>
-- MAGIC   <li>FloorSpaceBucket: 専有面積をカテゴリ化 (10段階)</li>
-- MAGIC   <li>AgeBucket: 築年数をカテゴリ化 (8段階)</li>
-- MAGIC   <li>DistanceBucket: 最寄り駅アクセス分数をカテゴリ化 (21段階)</li>
-- MAGIC </ul>
-- MAGIC </span>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 探索的データ分析 (EDA)
-- MAGIC <head>
-- MAGIC   <link href="https://fonts.googleapis.com/css2?family=Kosugi+Maru&display=swap" rel="stylesheet">
-- MAGIC   <style>
-- MAGIC     * {font-family: "Kosugi Maru", sans-serif !important;}
-- MAGIC   </style>
-- MAGIC </head>
-- MAGIC 
-- MAGIC <p>まず異なるアングルから特徴量と目標変数(Price)間の相関性、分布性質、線形関係など探索し、理解を深めていきます。<br>
-- MAGIC ここでは<span style="color:#38a">SQL</span>と<span style="color:#38a">ドラッグ&amp;ドロップ</span>のみでEDAを行っています:</p>
-- MAGIC 
-- MAGIC <h3>一部解析の例</h3>
-- MAGIC <div style="padding:10px 0;">
-- MAGIC <img src="https://jixjiadatabricks.blob.core.windows.net/dbfs-mnt/projects/suumo-property-price-assessment/price-age-bucket.gif" width="700px">
-- MAGIC <img src="https://jixjiadatabricks.blob.core.windows.net/dbfs-mnt/projects/suumo-property-price-assessment/price-age-count.gif" width="700px">
-- MAGIC </div>
-- MAGIC <div style="padding:10px 0;">
-- MAGIC <img src="https://jixjiadatabricks.blob.core.windows.net/dbfs-mnt/projects/suumo-property-price-assessment/price-distance.gif" width="700px">
-- MAGIC <img src="https://jixjiadatabricks.blob.core.windows.net/dbfs-mnt/projects/suumo-property-price-assessment/price-floor-space.gif" width="700px">
-- MAGIC </div>
-- MAGIC <div style="padding:10px 0;">
-- MAGIC <img src="https://jixjiadatabricks.blob.core.windows.net/dbfs-mnt/projects/suumo-property-price-assessment/price-municipality.gif" width="700px">
-- MAGIC <img src="https://jixjiadatabricks.blob.core.windows.net/dbfs-mnt/projects/suumo-property-price-assessment/price-distance-method.gif" width="700px">
-- MAGIC </div>
-- MAGIC 
-- MAGIC <div style="padding:10px 0;">
-- MAGIC   <h2>目標変数の密度分布 (Bin=50)</h2>
-- MAGIC <img src="https://jixjiadatabricks.blob.core.windows.net/dbfs-mnt/projects/suumo-property-price-assessment/price-distribution.gif" width="1200px">
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### SQLを使ったデータ可視化
-- MAGIC 
-- MAGIC SQLでレコードを取得したり、display関数でデータフレームを表示した際には、ビルトインされている可視化機能を活用できます。グラフボタンを押すことでグラフを表示できます。
-- MAGIC 
-- MAGIC ![](https://docs.databricks.com/_images/display-charts.png)
-- MAGIC 
-- MAGIC [Databricksにおけるデータの可視化 \- Qiita](https://qiita.com/taka_yayoi/items/36a307e79e9433121c38)

-- COMMAND ----------

-- DBTITLE 0,SQLを使ったデータ可視化
-- MAGIC %sql
-- MAGIC -- シンプルなクエリーを実行した後でグラフボタンを押します。
-- MAGIC SELECT * FROM property_kanto_delta order by age

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 数値型特徴量の統計情報

-- COMMAND ----------

-- DBTITLE 0,数値型特徴量の統計情報
-- MAGIC %python
-- MAGIC # summaryメソッドで統計情報を表示することもできます
-- MAGIC display(sql('Select Price, Distance_min, FloorSpace_m2, OtherSpace_m2, Age From property_data_kanto').summary())

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Deltaファイルの実体は？
-- MAGIC 
-- MAGIC `dbutils`はDatabricksのユーティリティライブラリです。以下の例ではファイルシステムを参照して、Deltaファイルの実態を確認しています。
-- MAGIC 
-- MAGIC [Databricksにおけるファイルシステム \- Qiita](https://qiita.com/taka_yayoi/items/e16c7272a7feb5ec9a92)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(dbutils.fs.ls(DELTALAKE_BRONZE_PATH))

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Deltaのメタデータ
-- MAGIC 
-- MAGIC `DESCRIBE DETAIL`でテーブルのメタデータを確認します。`format`が`delta`になっています。

-- COMMAND ----------

DESCRIBE detail property_kanto_delta

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Deltaの履歴
-- MAGIC 
-- MAGIC `DESCRIBE HISTORY`でテーブルの変更履歴を参照できます。

-- COMMAND ----------

DESCRIBE history property_kanto_delta

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### タイムトラベルを実行
-- MAGIC Deltaテーブルに対する操作はすべて記録されます。特定のバージョンのDeltaテーブルを参照することができます。これがタイムトラベルです。`VERSION AS OF`で参照したいバージョン番号を指定します。

-- COMMAND ----------

SELECT * FROM property_kanto_delta Version AS of 0

-- COMMAND ----------

-- MAGIC %md ## GitHub連携
-- MAGIC 
-- MAGIC Reposを活用することで、DatabricksノートブックをGitHubと連携することができます。Reposは、Gitのリポジトリと同期することでコンテンツの共同バージョン管理を可能とするフォルダーです。ReposにはDatabricksノートブックとサブフォルダのみを含めることができます。Gitリポジトリに任意のファイルを格納することはできますが、Databricksのワークスペースには表示されません。
-- MAGIC 
-- MAGIC > **注意**<br>
-- MAGIC > Reposを利用する際には、管理者によってRepos機能を有効化する必要があります。
-- MAGIC 
-- MAGIC [Repos for Git integration \| Databricks on AWS](https://docs.databricks.com/repos.html)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### GitHubの設定
-- MAGIC 
-- MAGIC 1. GitHubのアカウントアイコンをクリックし**Setting**を開きます。<br>
-- MAGIC ![](https://sajpstorage.blob.core.windows.net/demo20210614-18/git_account.png)
-- MAGIC 
-- MAGIC 1. **Developer settings > Personal access tokens**を開きます。<br>
-- MAGIC ![](https://sajpstorage.blob.core.windows.net/demo20210614-18/pesonal_access_token.png)
-- MAGIC 
-- MAGIC 1. **Generate new token**をクリックし、トークン名を指定し、repoにチェックを入れてトークンを作成します。トークンをコピーしておきます。<br>
-- MAGIC ![](https://sajpstorage.blob.core.windows.net/demo20210614-18/create_token.png)
-- MAGIC 
-- MAGIC 1. Databricksの**User Settings > Git Integration**を開き、**Git provider**はGitHub、**Token or app password**にコピーしたトークンを貼り付けます。**Git provider username or email**にはGitHubでのユーザー名もしくはメールアドレスを指定し、**Save**をクリックします。<br>
-- MAGIC ![](https://sajpstorage.blob.core.windows.net/demo20210614-18/token_setting.png)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Reposの作成
-- MAGIC 
-- MAGIC 1. サイドバーのReposをクリックし、**Add Repo**をクリックします。<br>
-- MAGIC ![](https://docs.databricks.com/_images/add-repo.png)
-- MAGIC 1. ダイアログでリポジトリのURL、リポジトリ名を指定します。<br>
-- MAGIC ![](https://sajpstorage.blob.core.windows.net/demo20210614-18/create_repo.png)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Reposの利用方法

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### NotebookレベルでのGit連携
-- MAGIC 
-- MAGIC 1. 画面右上の**Revision history**をクリックします。<br>
-- MAGIC ![](https://sajpstorage.blob.core.windows.net/demo20210614-18/revision_history.png)
-- MAGIC 
-- MAGIC 1. **Git: Not Linked**をクリックします。
-- MAGIC 1. GitのURLを指定して連携します。<br>
-- MAGIC ![](https://sajpstorage.blob.core.windows.net/demo20210614-18/git_integration.png)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## その他
-- MAGIC <head>
-- MAGIC   <link href="https://fonts.googleapis.com/css2?family=Kosugi+Maru&display=swap" rel="stylesheet">
-- MAGIC   <style>
-- MAGIC     h1,h2,h3,p,span,td, div {font-family: "Kosugi Maru", sans-serif !important;}
-- MAGIC   </style>
-- MAGIC </head>
-- MAGIC 
-- MAGIC <div style='line-height:1.5rem; padding-top: 10px;'>
-- MAGIC (1) コラボレーション機能：　他の人のNotobookを参照してみる。<br>
-- MAGIC (2) コメントを記述してみる。<br>
-- MAGIC (3) Notebookの改訂履歴を見てみる。<br>
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Notebookでのコラボレーション
-- MAGIC <p  style="position:relative;width: 1000px;height: 450px;overflow: hidden;">
-- MAGIC <img style="margin-top:25px;" src="https://psajpstorage.blob.core.windows.net/commonfiles/Collaboration001.gif" width="1000">
-- MAGIC </p>
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # END
-- MAGIC <head>
-- MAGIC   <link href="https://fonts.googleapis.com/css2?family=Kosugi+Maru&display=swap" rel="stylesheet">
-- MAGIC   <style>
-- MAGIC     h1,h2,h3,p,span,td, div {font-family: "Kosugi Maru", sans-serif !important;}
-- MAGIC   </style>
-- MAGIC </head>