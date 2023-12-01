# Databricks notebook source
from pyspark.sql.functions import col, to_timestamp
from datetime import datetime
from pyspark.sql.functions import col, lit, count, when, to_timestamp, monotonically_increasing_id, sum, to_date, row_number
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.window import Window

# COMMAND ----------

storage_account_name = "" #storage account name goes here
filesystem_name = "" #container name goes here
access_key = "" #access key goes here

spark.conf.set(f"fs.azure.account.key.{storage_account_name}.dfs.core.windows.net", access_key)

# COMMAND ----------

file_format_parquet = "parquet"  # e.g., "csv", "parquet", "json", etc.
file_format_csv = "csv"

# COMMAND ----------

file_path_ban = f"abfss://{filesystem_name}@{storage_account_name}.dfs.core.windows.net/output/ban_events/ban_events.csv"
file_path_channel = f"abfss://{filesystem_name}@{storage_account_name}.dfs.core.windows.net/output/channel/channels.csv"
file_path_chat_stats = f"abfss://{filesystem_name}@{storage_account_name}.dfs.core.windows.net/output/chat_stats/chat_stats.csv"
file_path_chat1 = f"abfss://{filesystem_name}@{storage_account_name}.dfs.core.windows.net/output/chat1/chat1.csv"
file_path_chat2 = f"abfss://{filesystem_name}@{storage_account_name}.dfs.core.windows.net/output/chat2/chat2.csv"
file_path_chat3 = f"abfss://{filesystem_name}@{storage_account_name}.dfs.core.windows.net/output/chat3/chat3.csv"
file_path_deletion = f"abfss://{filesystem_name}@{storage_account_name}.dfs.core.windows.net/output/deletion/deletion.csv"
file_path_superchat_stats = f"abfss://{filesystem_name}@{storage_account_name}.dfs.core.windows.net/output/superchat_stats/superchat_stats.csv"
file_path_superchat1 = f"abfss://{filesystem_name}@{storage_account_name}.dfs.core.windows.net/output/superchat1/superchat1.csv"
file_path_superchat2 = f"abfss://{filesystem_name}@{storage_account_name}.dfs.core.windows.net/output/superchat2/superchat2.csv"
file_path_superchat3 = f"abfss://{filesystem_name}@{storage_account_name}.dfs.core.windows.net/output/superchat3/superchat3.csv"

# COMMAND ----------

df_ban = spark.read.format(file_format_csv).option("header", "true").option("inferSchema", "true").load(file_path_ban)

# COMMAND ----------

df_channel = spark.read.format(file_format_csv).option("header", "true").option("inferSchema", "true").load(file_path_channel)

# COMMAND ----------

df_chat_stats = spark.read.format(file_format_csv).option("header", "true").option("inferSchema", "true").load(file_path_chat_stats)

# COMMAND ----------

df_chat1 = spark.read.format(file_format_csv).option("header", "true").option("inferSchema", "true").load(file_path_chat1)

# COMMAND ----------

df_chat2 = spark.read.format(file_format_csv).option("header", "true").option("inferSchema", "true").load(file_path_chat2)

# COMMAND ----------

df_chat3 = spark.read.format(file_format_csv).option("header", "true").option("inferSchema", "true").load(file_path_chat3)

# COMMAND ----------

df_deletion = spark.read.format(file_format_csv).option("header", "true").option("inferSchema", "true").load(file_path_deletion)

# COMMAND ----------

df_superchat_stats = spark.read.format(file_format_csv).option("header", "true").option("inferSchema", "true").load(file_path_superchat_stats)

# COMMAND ----------

df_superchat1 = spark.read.format(file_format_csv).option("header", "true").option("inferSchema", "true").load(file_path_superchat1)

# COMMAND ----------

df_superchat2 = spark.read.format(file_format_csv).option("header", "true").option("inferSchema", "true").load(file_path_superchat2)

# COMMAND ----------

df_superchat3 = spark.read.format(file_format_csv).option("header", "true").option("inferSchema", "true").load(file_path_superchat3)

# COMMAND ----------

# DBTITLE 1,Merging data
display(df_chat1)

# COMMAND ----------

display(df_chat2)

# COMMAND ----------

display(df_chat3)

# COMMAND ----------

df_chat = df_chat1.union(df_chat2).union(df_chat3)

# COMMAND ----------

display(df_chat)

# COMMAND ----------

df_deletion_filtered_test = df_deletion.dropDuplicates(["videoId", "channelId"])

# COMMAND ----------

deletion_cols = df_deletion_filtered_test.select("videoId", "channelId", "deleted_by_mod")

df_chat_merged =  df_chat.join(deletion_cols, on=["videoId", "channelId"], how="left")

# COMMAND ----------

df_chat_merged = df_chat_merged.na.fill({"deleted_by_mod": False})

# COMMAND ----------

print('Total combined: ', df_chat_merged.count())

# COMMAND ----------

display(df_chat_merged)

# COMMAND ----------

display(df_ban)

# COMMAND ----------

df_ban = df_ban.dropDuplicates(["authorChannelId", "videoId"])

# COMMAND ----------

banned_deletion_cols = df_ban.select("authorChannelId", "videoId", "banned")

df_chat_merged = df_chat_merged.join(banned_deletion_cols, on=['authorChannelId', 'videoId'], how='left')

# COMMAND ----------

df_chat_merged = df_chat_merged.na.fill({"banned": False})

# COMMAND ----------

display(df_chat_merged)

# COMMAND ----------

display(df_chat_merged.count())

# COMMAND ----------

display(df_channel)

# COMMAND ----------

channel_deletion_cols = df_channel.select("channelId", "englishName", "affiliation")

df_chat_channel_merged = df_chat_merged.join(channel_deletion_cols, on="channelId", how="inner")

# COMMAND ----------

display(df_chat_channel_merged)

# COMMAND ----------

display(df_chat_channel_merged.count())

# COMMAND ----------

df_superchat_merged = df_superchat1.union(df_superchat2).union(df_superchat3)

# COMMAND ----------

display(df_superchat_merged)

# COMMAND ----------

print('Superchat len: ', df_superchat1.count() + df_superchat2.count() + df_superchat3.count())

# COMMAND ----------

print('Superchat merged len: ',df_superchat_merged.count())

# COMMAND ----------

superchat_deletion_cols = df_channel.select("channelId", "englishName", "affiliation")

df_superchat_channel_merged = df_superchat_merged.join(superchat_deletion_cols, on="channelId", how="inner")

# COMMAND ----------

display(df_superchat_channel_merged)

# COMMAND ----------

display(df_superchat_channel_merged.count())

# COMMAND ----------

display(df_chat_stats)

# COMMAND ----------

chat_stats_deletion_cols = df_channel.select("channelId", "englishName", "affiliation")

df_chat_stats_channel_merged = df_chat_stats.join(chat_stats_deletion_cols, on="channelId", how="inner")

# COMMAND ----------

display(df_chat_stats_channel_merged)

# COMMAND ----------

display(df_superchat_stats)

# COMMAND ----------

superchat_stats_deletion_cols = df_channel.select("channelId", "englishName", "affiliation")

df_superchat_stats_channel_merged = df_superchat_stats.join(superchat_stats_deletion_cols, on="channelId", how="inner")

# COMMAND ----------

display(df_superchat_stats_channel_merged)

# COMMAND ----------

# DBTITLE 1,Saving final dataframe
storage_account_name = "" #storage account name goes here
filesystem_name = "" #container name goes here

# COMMAND ----------

output_path_chat_channel = f"abfss://{filesystem_name}@{storage_account_name}.dfs.core.windows.net/final/chat_channel_merged"
df_chat_channel_merged.coalesce(1).write.option("header", "true").csv(output_path_chat_channel)

# COMMAND ----------

output_path_superchat_channel = f"abfss://{filesystem_name}@{storage_account_name}.dfs.core.windows.net/final/superchat_channel_merged"
df_superchat_channel_merged.coalesce(1).write.option("header", "true").csv(output_path_superchat_channel)

# COMMAND ----------

output_path_chat_stats_channel = f"abfss://{filesystem_name}@{storage_account_name}.dfs.core.windows.net/final/chat_stats_channel"
df_chat_stats_channel_merged.coalesce(1).write.option("header", "true").csv(output_path_chat_stats_channel)

# COMMAND ----------

output_path_superchat_stats_channel = f"abfss://{filesystem_name}@{storage_account_name}.dfs.core.windows.net/final/superchat_stats_channel"
df_superchat_stats_channel_merged.coalesce(1).write.option("header", "true").csv(output_path_superchat_stats_channel)

# COMMAND ----------

# DBTITLE 1,EDA

