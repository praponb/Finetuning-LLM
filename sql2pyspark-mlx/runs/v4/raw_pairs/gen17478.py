from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("merchant").orderBy("txn_time", "id")
w2 = Window.partitionBy("merchant").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_transactions
    .filter(F.col("merchant").isNotNull())
    .select(
        "*",
        F.date_trunc("WEEK", "txn_time").alias("txn_time_period"),
        F.upper("merchant").alias("merchant_upper"),
    )
)

flagged = (
    cleaned
    .withColumn("new_group_flag", F.when(F.lag("txn_time").over(w).isNull() | (F.col("txn_time") > F.lag("txn_time").over(w) + F.expr("INTERVAL 60 DAYS")), 1).otherwise(0))
)

sessions_transactions = flagged.withColumn("burst_id", F.sum("new_group_flag").over(w2))

result = (
    sessions_transactions
    .groupBy(
        F.date_trunc("MONTH", "txn_time").alias("activity_month"),
        F.date_trunc("QUARTER", "txn_time").alias("quarter_start"),
    )
    .agg(
        ((F.unix_timestamp(F.max("txn_time")) - F.unix_timestamp(F.min("txn_time"))) / 60).alias("span_minutes"),
        F.min("new_group_flag").alias("min_new_group_flag"),
        F.count(F.when(F.col("merchant") == "Amazon", 1)).alias("amazon_count"),
    )
    .filter(F.col("span_minutes") > 0)
)
