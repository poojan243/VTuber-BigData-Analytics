# Databricks notebook source
from pyspark.sql.functions import col, to_timestamp
from datetime import datetime
from pyspark.sql.functions import col, lit, count, when, to_timestamp, monotonically_increasing_id, sum, to_date, row_number
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.window import Window

# COMMAND ----------

# DBTITLE 1,Setting up file storage credentials
storage_account_name = "" #storage account name goes here
filesystem_name = "" #container name goes here
access_key = "" #access key goes here

spark.conf.set(f"fs.azure.account.key.{storage_account_name}.dfs.core.windows.net", access_key)

# COMMAND ----------

file_format_parquet = "parquet"  # e.g., "csv", "parquet", "json", etc.
file_format_csv = "csv"

file_path_ban = f"abfss://{filesystem_name}@{storage_account_name}.dfs.core.windows.net/ban_events.parquet"
file_path_channel = f"abfss://{filesystem_name}@{storage_account_name}.dfs.core.windows.net/channels.csv"
file_path_chat_stats = f"abfss://{filesystem_name}@{storage_account_name}.dfs.core.windows.net/chat_stats.csv"
file_path_chat1 = f"abfss://{filesystem_name}@{storage_account_name}.dfs.core.windows.net/chats_2022-01.parquet"
file_path_chat2 = f"abfss://{filesystem_name}@{storage_account_name}.dfs.core.windows.net/chats_2022-02.parquet"
file_path_chat3 = f"abfss://{filesystem_name}@{storage_account_name}.dfs.core.windows.net/chats_2022-03.parquet"
file_path_deletion = f"abfss://{filesystem_name}@{storage_account_name}.dfs.core.windows.net/deletion_events.parquet"
file_path_superchat_stats = f"abfss://{filesystem_name}@{storage_account_name}.dfs.core.windows.net/superchat_stats.csv"
file_path_superchat1 = f"abfss://{filesystem_name}@{storage_account_name}.dfs.core.windows.net/superchats_2022-01.parquet"
file_path_superchat2 = f"abfss://{filesystem_name}@{storage_account_name}.dfs.core.windows.net/superchats_2022-02.parquet"
file_path_superchat3 = f"abfss://{filesystem_name}@{storage_account_name}.dfs.core.windows.net/superchats_2022-03.parquet"

# COMMAND ----------

# DBTITLE 1,Reading all files
df_ban = spark.read.format(file_format_parquet).load(file_path_ban)

# COMMAND ----------

df_channel = spark.read.format(file_format_csv).load(file_path_channel)

# COMMAND ----------

df_chat_stats = spark.read.format(file_format_csv).option("header", "true").option("inferSchema", "true").load(file_path_chat_stats)

# COMMAND ----------

df_chat1 = spark.read.format(file_format_parquet).load(file_path_chat1)

# COMMAND ----------

df_chat2 = spark.read.format(file_format_parquet).load(file_path_chat2)

# COMMAND ----------

df_chat3 = spark.read.format(file_format_parquet).load(file_path_chat3)

# COMMAND ----------

df_deletion = spark.read.format(file_format_parquet).load(file_path_deletion)

# COMMAND ----------

df_superchat_stats = spark.read.format(file_format_csv).option("header", "true").option("inferSchema", "true").load(file_path_superchat_stats)

# COMMAND ----------

df_superchat1 = spark.read.format(file_format_parquet).load(file_path_superchat1)

# COMMAND ----------

df_superchat2 = spark.read.format(file_format_parquet).load(file_path_superchat2)

# COMMAND ----------

df_superchat3 = spark.read.format(file_format_parquet).load(file_path_superchat3)

# COMMAND ----------

# DBTITLE 1,Displaying dataframes
display(df_ban.head(10))

# COMMAND ----------

display(df_channel.head(10))

# COMMAND ----------

display(df_chat_stats.head(10))

# COMMAND ----------

display(df_chat1.head(10))

# COMMAND ----------

display(df_chat2)

# COMMAND ----------

display(df_chat3)

# COMMAND ----------

display(df_chat3.tail(10))

# COMMAND ----------

display(df_deletion.head(10))

# COMMAND ----------

display(df_superchat_stats.head(10))

# COMMAND ----------

display(df_superchat1.head(10))

# COMMAND ----------

display(df_superchat2)

# COMMAND ----------

display(df_superchat3)

# COMMAND ----------

# DBTITLE 1,Data Cleaning
start_date = datetime(2022, 1, 1)
end_date = datetime(2022, 3, 31, 23, 59, 59)

df_ban = df_ban.withColumn("timestamp", to_timestamp("timestamp"))
df_ban_filtered = df_ban.filter((col("timestamp") >= start_date) & (col("timestamp") <= end_date))

# COMMAND ----------

display(df_ban_filtered.tail(10))

# COMMAND ----------

start_period = "2022-01"
end_period = "2022-03"

# Filter the DataFrame
df_chat_stats_filtered = df_chat_stats.filter((col("period") >= start_period) & (col("period") <= end_period))
df_chat_stats_filtered = df_chat_stats_filtered.withColumn("period", col("period").substr(1, 7))

# COMMAND ----------

display(df_chat_stats_filtered)

# COMMAND ----------

start_date = datetime(2022, 1, 1)
end_date = datetime(2022, 3, 31, 23, 59, 59)

df_deletion = df_deletion.withColumn("timestamp", to_timestamp("timestamp"))
df_deletion_filtered = df_deletion.filter((col("timestamp") >= start_date) & (col("timestamp") <= end_date))

# COMMAND ----------

display(df_deletion_filtered)

# COMMAND ----------

start_period = "2022-01"
end_period = "2022-03"

# Filter the DataFrame
df_superchat_stats_filtered = df_superchat_stats.filter((col("period") >= start_period) & (col("period") <= end_period))
df_superchat_stats_filtered = df_superchat_stats_filtered.withColumn("period", col("period").substr(1, 7))

# COMMAND ----------

display(df_superchat_stats_filtered.tail(10))

# COMMAND ----------

print('Chat 1', df_chat1.count())
print('Chat 2', df_chat2.count())
print('Chat 3', df_chat3.count())

# COMMAND ----------

null_counts_ban = df_ban_filtered.select([count(when(col(c).isNull(), c)).alias(c) for c in df_ban_filtered.columns])
null_counts_ban.show()

# COMMAND ----------

null_counts_channel = df_channel.select([count(when(col(c).isNull(), c)).alias(c) for c in df_channel.columns])
null_counts_channel.show()

# COMMAND ----------

display(df_channel.head(1))

# COMMAND ----------

column_names = ["channelId", "name", "englishName", "affiliation", "group", "subscriptionCount", "videoCount", "photo"]
df_channel = df_channel.toDF(*column_names)


# COMMAND ----------

display(df_channel)

# COMMAND ----------

first_row = df_channel.limit(1)

# Subtract the first row from the DataFrame
df_channel = df_channel.subtract(first_row)

# COMMAND ----------

df_channel = df_channel.drop("name","group","photo")

# COMMAND ----------

display(df_channel)

# COMMAND ----------

null_counts_chat_stats = df_chat_stats_filtered.select([count(when(col(c).isNull(), c)).alias(c) for c in df_chat_stats_filtered.columns])
null_counts_chat_stats.show()

# COMMAND ----------

null_counts_chat1 = df_chat1.select([count(when(col(c).isNull(), c)).alias(c) for c in df_chat1.columns])
null_counts_chat1.show()

# COMMAND ----------

counts = df_chat1.agg(
    count(when(col("isMember") == True, True)).alias("True_Count"),
    count(when(col("isMember") == False, True)).alias("False_Count"),
    count(when(col("isMember").isNull(), True)).alias("Null_Count")
)

counts.show()

# COMMAND ----------

df_chat1 = df_chat1.na.fill({"isMember": False}) 

# COMMAND ----------

null_counts_chat1 = df_chat1.select([count(when(col(c).isNull(), c)).alias(c) for c in df_chat1.columns])
null_counts_chat1.show()

# COMMAND ----------

null_counts_chat2 = df_chat2.select([count(when(col(c).isNull(), c)).alias(c) for c in df_chat2.columns])
null_counts_chat2.show()

# COMMAND ----------

null_counts_chat3 = df_chat3.select([count(when(col(c).isNull(), c)).alias(c) for c in df_chat3.columns])
null_counts_chat3.show()

# COMMAND ----------

null_counts_deletion = df_deletion_filtered.select([count(when(col(c).isNull(), c)).alias(c) for c in df_deletion_filtered.columns])
null_counts_deletion.show()

# COMMAND ----------

null_counts_superchatstats = df_superchat_stats_filtered.select([count(when(col(c).isNull(), c)).alias(c) for c in df_superchat_stats_filtered.columns])
null_counts_superchatstats.show()

# COMMAND ----------

null_counts_superchat1 = df_superchat1.select([count(when(col(c).isNull(), c)).alias(c) for c in df_superchat1.columns])
null_counts_superchat1.show()

# COMMAND ----------

null_counts_superchat2 = df_superchat2.select([count(when(col(c).isNull(), c)).alias(c) for c in df_superchat2.columns])
null_counts_superchat2.show()

# COMMAND ----------

null_counts_superchat3 = df_superchat3.select([count(when(col(c).isNull(), c)).alias(c) for c in df_superchat3.columns])
null_counts_superchat3.show()

# COMMAND ----------

df_superchat1 = df_superchat1.withColumn(
    "color",
    when(col("significance") == 1, "blue")
    .when(col("significance") == 2, "lightblue")
    .when(col("significance") == 3, "green")
    .when(col("significance") == 4, "yellow")
    .when(col("significance") == 5, "orange")
    .when(col("significance") == 6, "magenta")
    .when(col("significance") == 7, "red")
    .otherwise("unknown")  # Handling cases where significance is outside 1-7
)

# COMMAND ----------

df_superchat2 = df_superchat2.withColumn(
    "color",
    when(col("significance") == 1, "blue")
    .when(col("significance") == 2, "lightblue")
    .when(col("significance") == 3, "green")
    .when(col("significance") == 4, "yellow")
    .when(col("significance") == 5, "orange")
    .when(col("significance") == 6, "magenta")
    .when(col("significance") == 7, "red")
    .otherwise("unknown")  # Handling cases where significance is outside 1-7
)

# COMMAND ----------

df_superchat3 = df_superchat3.withColumn(
    "color",
    when(col("significance") == 1, "blue")
    .when(col("significance") == 2, "lightblue")
    .when(col("significance") == 3, "green")
    .when(col("significance") == 4, "yellow")
    .when(col("significance") == 5, "orange")
    .when(col("significance") == 6, "magenta")
    .when(col("significance") == 7, "red")
    .otherwise("unknown")  # Handling cases where significance is outside 1-7
)

# COMMAND ----------

significance_counts = df_superchat1.groupBy("color").agg(count("*").alias("count")).orderBy("color")

# Show the result
significance_counts.show()

# COMMAND ----------

# DBTITLE 1,Deleting chats to reduce data
print('Chat 1', df_chat1.count())
print('Chat 2', df_chat2.count())
print('Chat 3', df_chat3.count())

# COMMAND ----------

df_chat1 = df_chat1.withColumn("date", to_date("timestamp"))

# COMMAND ----------

windowSpec = Window.partitionBy("date").orderBy("timestamp")

# Add a row number for each record within each date
df_chat1 = df_chat1.withColumn("row_num", row_number().over(windowSpec))

# Filter to keep only the first N records for each date
N = 100000  # For example, keeping the first 100000 records per day
df_chat1_filtered = df_chat1.filter(col("row_num") <= N)

# COMMAND ----------

df_chat1_filtered = df_chat1_filtered.drop("date","row_num")

# COMMAND ----------

display(df_chat1_filtered.count())

# COMMAND ----------

df_chat2 = df_chat2.withColumn("date", to_date("timestamp"))

# COMMAND ----------

windowSpec = Window.partitionBy("date").orderBy("timestamp")

# Add a row number for each record within each date
df_chat2 = df_chat2.withColumn("row_num", row_number().over(windowSpec))

# Filter to keep only the first N records for each date
N = 100000  # For example, keeping the first 100000 records per day
df_chat2_filtered = df_chat2.filter(col("row_num") <= N)

# COMMAND ----------

df_chat2_filtered = df_chat2_filtered.drop("date","row_num")

# COMMAND ----------

display(df_chat2_filtered.count())

# COMMAND ----------

df_chat3 = df_chat3.withColumn("date", to_date("timestamp"))

# COMMAND ----------

windowSpec = Window.partitionBy("date").orderBy("timestamp")

# Add a row number for each record within each date
df_chat3 = df_chat3.withColumn("row_num", row_number().over(windowSpec))

# Filter to keep only the first N records for each date
N = 100000  # For example, keeping the first 100000 records per day
df_chat3_filtered = df_chat3.filter(col("row_num") <= N)

# COMMAND ----------

df_chat3_filtered = df_chat3_filtered.drop("date","row_num")

# COMMAND ----------

display(df_chat3_filtered.count())

# COMMAND ----------

# DBTITLE 1,Merging data
df_chat1_filtered = df_chat1_filtered.withColumn("chatId", monotonically_increasing_id())

# COMMAND ----------

display(df_chat1_filtered)

# COMMAND ----------

# Assuming the row counts are given as follows
count_df_chat_filtered1 = 3100000 
count_df_chat_filtered2 = 2800000

# Calculate the offset for df_chat2 and df_chat3
offset_df_chat_filtered2 = count_df_chat_filtered1
offset_df_chat_filtered3 = count_df_chat_filtered1 + count_df_chat_filtered2

# COMMAND ----------

df_chat2_filtered = df_chat2_filtered.withColumn("chatId", monotonically_increasing_id() + offset_df_chat_filtered2)
df_chat3_filtered = df_chat3_filtered.withColumn("chatId", monotonically_increasing_id() + offset_df_chat_filtered3)

# COMMAND ----------

display(df_chat2_filtered)

# COMMAND ----------

display(df_chat3_filtered)

# COMMAND ----------

print('Superchat 1', df_superchat1.count())
print('Superchat 2', df_superchat2.count())
print('Superchat 3', df_superchat3.count())

# COMMAND ----------

count_df_chat_filtered1 = 3100000
count_df_chat_filtered2 = 2800000
count_df_chat_filtered3 = 3100000
count_df_superchat1 = 392073
count_df_superchat2 = 302486
count_df_superchat3 = 338877

offset_df_chat2 = count_df_chat_filtered1
offset_df_chat3 = offset_df_chat2 + count_df_chat_filtered2
offset_df_superchat1 = offset_df_chat3 + count_df_chat_filtered3
offset_df_superchat2 = offset_df_superchat1 + count_df_superchat1
offset_df_superchat3 = offset_df_superchat2 + count_df_superchat2

# COMMAND ----------

df_superchat1 = df_superchat1.withColumn("superChatId", monotonically_increasing_id() + offset_df_superchat1)
df_superchat2 = df_superchat2.withColumn("superChatId", monotonically_increasing_id() + offset_df_superchat2)
df_superchat3 = df_superchat3.withColumn("superChatId", monotonically_increasing_id() + offset_df_superchat3)

# COMMAND ----------

display(df_superchat1)

# COMMAND ----------

df_chat_stats_filtered = df_chat_stats_filtered.withColumn("chatStatsId", monotonically_increasing_id())

# COMMAND ----------

display(df_chat_stats_filtered)

# COMMAND ----------

df_superchat_stats_filtered = df_superchat_stats_filtered.withColumn("superChatStatsId", monotonically_increasing_id())

# COMMAND ----------

display(df_superchat_stats_filtered)

# COMMAND ----------

display(df_deletion_filtered)

# COMMAND ----------

df_deletion_filtered = df_deletion_filtered.withColumn("deleted_by_mod", when(col("retracted") == True, True).otherwise(False))

# COMMAND ----------

display(df_deletion_filtered)

# COMMAND ----------

true_false_counts = df_deletion_filtered.groupBy("deleted_by_mod").agg(count("*").alias("count")).orderBy("deleted_by_mod")

# Show the result
true_false_counts.show()

# COMMAND ----------

display(df_ban_filtered)

# COMMAND ----------

# Add a new column 'banned' and set its value to True for all rows
df_ban_filtered = df_ban_filtered.withColumn("banned", lit(True))

# COMMAND ----------

display(df_ban_filtered)

# COMMAND ----------

# DBTITLE 1,Downloading cleaned Data
storage_account_name = "" #storage account name goes here
filesystem_name = "" #container name goes here

# COMMAND ----------

output_path_ban = f"abfss://{filesystem_name}@{storage_account_name}.dfs.core.windows.net/output/ban_events"
df_ban_filtered.coalesce(1).write.mode("overwrite").option("header", "true").csv(output_path_ban)

# COMMAND ----------

output_path_channel = f"abfss://{filesystem_name}@{storage_account_name}.dfs.core.windows.net/output/channel"
df_channel.coalesce(1).write.option("header", "true").csv(output_path_channel)

# COMMAND ----------

output_path_chat_stats = f"abfss://{filesystem_name}@{storage_account_name}.dfs.core.windows.net/output/chat_stats"
df_chat_stats_filtered.coalesce(1).write.option("header", "true").csv(output_path_chat_stats)

# COMMAND ----------

output_path_chat1 = f"abfss://{filesystem_name}@{storage_account_name}.dfs.core.windows.net/output/chat1"
df_chat1_filtered.coalesce(1).write.option("header", "true").csv(output_path_chat1)

# COMMAND ----------

output_path_chat2 = f"abfss://{filesystem_name}@{storage_account_name}.dfs.core.windows.net/output/chat2"
df_chat2_filtered.coalesce(1).write.option("header", "true").csv(output_path_chat2)

# COMMAND ----------

output_path_chat3 = f"abfss://{filesystem_name}@{storage_account_name}.dfs.core.windows.net/output/chat3"
df_chat3_filtered.coalesce(1).write.option("header", "true").csv(output_path_chat3)

# COMMAND ----------

output_path_deletion = f"abfss://{filesystem_name}@{storage_account_name}.dfs.core.windows.net/output/deletion"
df_deletion_filtered.coalesce(1).write.option("header", "true").csv(output_path_deletion)

# COMMAND ----------

output_path_superchat_stats = f"abfss://{filesystem_name}@{storage_account_name}.dfs.core.windows.net/output/superchat_stats"
df_superchat_stats_filtered.coalesce(1).write.option("header", "true").csv(output_path_superchat_stats)

# COMMAND ----------

output_path_superchat1 = f"abfss://{filesystem_name}@{storage_account_name}.dfs.core.windows.net/output/superchat1"
df_superchat1.coalesce(1).write.option("header", "true").csv(output_path_superchat1)

# COMMAND ----------

output_path_superchat2 = f"abfss://{filesystem_name}@{storage_account_name}.dfs.core.windows.net/output/superchat2"
df_superchat2.coalesce(1).write.option("header", "true").csv(output_path_superchat2)

# COMMAND ----------

output_path_superchat3 = f"abfss://{filesystem_name}@{storage_account_name}.dfs.core.windows.net/output/superchat3"
df_superchat3.coalesce(1).write.option("header", "true").csv(output_path_superchat3)
