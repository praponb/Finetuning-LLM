from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("merchant").orderBy("txn_time", "id")
w2 = Window.partitionBy("merchant").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_transactions
    .filter(F.col("txn_time") > F.to_timestamp(F.lit("2023-07-04 22:00:00")))
    .select(
        "*",
        F.dayofmonth("txn_time").alias("txn_time_day"),
        F.coalesce("merchant", F.lit("NONE")).alias("resolved_merchant"),
    )
)

flagged = (
    cleaned
    .withColumn("new_group_flag", F.when(F.lag("txn_time").over(w).isNull() | (F.col("txn_time") > F.lag("txn_time").over(w) + F.expr("INTERVAL 14 DAYS")), 1).otherwise(0))
)

sessionized = flagged.withColumn("island_id", F.sum("new_group_flag").over(w2))

result = (
    sessionized
    .withColumn("week_start", F.date_trunc("WEEK", "txn_time"))
    .rollup("week_start")
    .agg(
        F.count(F.when(F.col("merchant") == "Shell", 1)).alias("shell_count"),
        F.sum(F.when(F.col("txn_type") == "FEE", F.col("new_group_flag")).otherwise(0)).alias("fee_new_group_flag"),
        F.count(F.when(F.col("merchant") == "Lazada", 1)).alias("lazada_rows"),
        F.grouping("week_start").alias("grouping_id"),
    )
)
