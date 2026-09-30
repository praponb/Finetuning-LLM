from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("txn_type").orderBy("txn_time_period", "id")
w2 = Window.partitionBy("txn_type").orderBy("txn_time_period", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_transactions
    .filter(F.col("is_flagged"))
    .select(
        "*",
        F.date_trunc("WEEK", "txn_time").alias("txn_time_period"),
        F.col("txn_time").cast("date").alias("txn_time_day"),
    )
)

flagged = (
    filtered
    .withColumn("session_start_flag", F.when(F.col("txn_time_period") - F.lag("txn_time_period").over(w) > F.expr("INTERVAL 30 DAYS"), 1).otherwise(0))
)

sessions_transactions = flagged.withColumn("burst_id", F.sum("session_start_flag").over(w2))

result = (
    sessions_transactions
    .rollup(F.date_trunc("WEEK", "txn_time_day").alias("week_start"), "burst_id")
    .agg(
        F.sort_array(F.collect_set("merchant")).alias("merchant_list"),
        F.round(F.avg("burst_id"), 2).alias("avg_burst_id"),
        F.count("*").alias("num_rows"),
        F.count_if(F.col("merchant") == "Amazon").alias("amazon_events"),
    )
    .orderBy(F.col("num_rows").desc_nulls_last())
)
