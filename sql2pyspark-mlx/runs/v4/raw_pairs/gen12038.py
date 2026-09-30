from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_id").orderBy("txn_time", "id")
w2 = Window.partitionBy("account_id").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_transactions
    .filter((F.col("txn_time") >= F.lit("2024-01-09 09:00:00").cast("timestamp")) & F.hour("txn_time").between(12, 15))
    .withColumn("txn_time_period", F.date_trunc("week", "txn_time"))
    .withColumn("merchant_clean", F.coalesce("merchant", F.lit("UNKNOWN")))
)

flagged = (
    filtered
    .withColumn("new_group_flag", F.when(F.col("txn_time") - F.lag("txn_time").over(w) > F.expr("INTERVAL 60 DAYS"), 1).otherwise(0))
)

islands = flagged.withColumn("burst_id", F.sum("new_group_flag").over(w2))

result = (
    islands
    .groupBy("burst_id")
    .agg(
        F.min("txn_time_period").alias("first_seen"),
        F.min("txn_time").alias("first_seen_2"),
        F.sum(F.when(F.col("merchant") == "Shell", F.col("burst_id")).otherwise(0)).alias("shell_burst_id"),
        F.sort_array(F.collect_set("merchant")).alias("merchant_list"),
    )
    .orderBy(F.col("shell_burst_id").asc_nulls_last())
)
