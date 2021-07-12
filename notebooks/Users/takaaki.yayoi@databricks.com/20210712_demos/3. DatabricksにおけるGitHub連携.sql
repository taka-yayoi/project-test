-- Databricks notebook source
-- MAGIC %md
-- MAGIC # DatabricksにおけるGitHub連携
-- MAGIC <head>
-- MAGIC   <link href="https://fonts.googleapis.com/css2?family=Kosugi+Maru&display=swap" rel="stylesheet">
-- MAGIC   <style>
-- MAGIC     h1,h2,h3,p,span,td, div {font-family: "Kosugi Maru", sans-serif !important;}
-- MAGIC   </style>
-- MAGIC </head>
-- MAGIC 
-- MAGIC 本デモでは、DatabricksにおけるGitHub連携機能をご紹介します。
-- MAGIC <table>
-- MAGIC   <tr><th>作者</th><th>Databricks Japan</th></tr>
-- MAGIC   <tr><td>日付</td><td>2021/07/09</td></tr>
-- MAGIC   <tr><td>バージョン</td><td>1.4</td></tr>
-- MAGIC   <tr><td>クラスター</td><td>8.3ML</td></tr>
-- MAGIC </table>
-- MAGIC <img style="margin-top:25px;" src="https://jixjiadatabricks.blob.core.windows.net/images/databricks-logo-small-new.png" width="140">

-- COMMAND ----------

-- MAGIC %md ## GitHub連携
-- MAGIC 
-- MAGIC Reposを活用することで、DatabricksノートブックをGitHubと連携することができます。Reposは、Gitのリポジトリと同期することでコンテンツの共同バージョン管理を可能とするフォルダーです。ReposにはDatabricksノートブックとサブフォルダのみを含めることができます。Gitリポジトリに任意のファイルを格納することはできますが、Databricksのワークスペースには表示されません。
-- MAGIC 
-- MAGIC 以下のGitプロバイダーをサポートしています。
-- MAGIC - GitHub
-- MAGIC - Bitbucket
-- MAGIC - GitLab
-- MAGIC 
-- MAGIC > **注意**<br>
-- MAGIC > Reposを利用する際には、管理者によってRepos機能を有効化する必要があります。
-- MAGIC 
-- MAGIC **参考資料**
-- MAGIC - [Databricks ReposによるGit連携 \- Qiita](https://qiita.com/taka_yayoi/items/b89f199ff0d3a4c16140)
-- MAGIC - [Repos for Git integration \| Databricks on AWS](https://docs.databricks.com/repos.html)

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
-- MAGIC 
-- MAGIC 複数人でコード管理を行う際にはReposの利用をお勧めします。
-- MAGIC 
-- MAGIC 1. Repo内のノートブックを編集します。<br>
-- MAGIC ![](https://sajpstorage.blob.core.windows.net/demo20210614-18/repo_change.png)
-- MAGIC 
-- MAGIC 1. Commitする際にはノートブック名の左にある**Gitブランチ名**のボタンをクリックします。変更箇所を確認しSummaryを入力して、**Commit & Push**をクリックします。<br>
-- MAGIC ![](https://sajpstorage.blob.core.windows.net/demo20210614-18/repos_commit.png)
-- MAGIC 
-- MAGIC 1. リポジトリが更新されます。　<br>
-- MAGIC ![](https://sajpstorage.blob.core.windows.net/demo20210614-18/git_history_2.png)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### リポジトリ上のライブラリのインポート
-- MAGIC 
-- MAGIC GitHubにコミットしたライブラリをインポートすることができます。<br><br>
-- MAGIC 
-- MAGIC 1. インポートする際にはGitHubのPersonal Access Tokenが必要となります。秘匿性の高い情報ですので、CLIを使用して事前にシークレットとして登録しておきおます。
-- MAGIC 2. 以下のように、pipでライブラリをインストールしてライブラリで定義されている関数を呼び出します。
-- MAGIC 
-- MAGIC [Databricksにおけるシークレットの管理 \- Qiita](https://qiita.com/taka_yayoi/items/338ef0c5394fe4eb87c0)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC 
-- MAGIC # シークレットからGitHubのパーソナルアクセストークンを取得
-- MAGIC token = dbutils.secrets.get(scope = "takaaki.yayoi@databricks.com", key = "github")
-- MAGIC 
-- MAGIC # pipを用いてライブラリをインストール
-- MAGIC %pip install git+https://$token@github.com/taka-yayoi/project-test.git

-- COMMAND ----------

-- MAGIC %python
-- MAGIC 
-- MAGIC import tasks
-- MAGIC tasks.test()

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### NotebookレベルでのGit連携
-- MAGIC 
-- MAGIC Repoではなく、各自のノートブックをGitに連携することも可能です。
-- MAGIC 
-- MAGIC 1. 画面右上の**Revision history**をクリックします。<br>
-- MAGIC ![](https://sajpstorage.blob.core.windows.net/demo20210614-18/revision_history.png)
-- MAGIC 
-- MAGIC 1. **Git: Not Linked**をクリックします。
-- MAGIC 1. GitのURLを指定して連携します。<br>
-- MAGIC ![](https://sajpstorage.blob.core.windows.net/demo20210614-18/git_integration.png)
-- MAGIC 1. 連携が完了すると**Git: Synced**と表示されます。<br>
-- MAGIC ![](https://sajpstorage.blob.core.windows.net/demo20210614-18/git_linked.png)
-- MAGIC 1. コミットしたいバージョンの**Save**リンクをクリックします。
-- MAGIC 1. Descriptionを記入してコミットします。<br>
-- MAGIC ![](https://sajpstorage.blob.core.windows.net/demo20210614-18/save_revision.png)
-- MAGIC 1. Gitのリポジトリに記録されます。<br>
-- MAGIC ![](https://sajpstorage.blob.core.windows.net/demo20210614-18/git_history.png)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # END
-- MAGIC <head>
-- MAGIC   <link href="https://fonts.googleapis.com/css2?family=Kosugi+Maru&display=swap" rel="stylesheet">
-- MAGIC   <style>
-- MAGIC     h1,h2,h3,p,span,td, div {font-family: "Kosugi Maru", sans-serif !important;}
-- MAGIC   </style>
-- MAGIC </head>