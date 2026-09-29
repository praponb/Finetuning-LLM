from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("txn_type").orderBy("txn_time", "id")
w2 = Window.partitionBy("txn_type").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_transactions
    .filter(F.col("merchant").isNotNull() & ~F.col("is_flagged"))
    .select(
        "id",
        "account_id",
        "txn_type",
        "amount",
        "txn_time",
        "merchant",
        F.col("txn_time").cast("date").alias("txn_time_day"),
    )
)

with_gaps = (
    filtered
    .withColumn("new_group_flag", F.when(F.lag("txn_time").over(w).isNull() | (F.col("txn_time") > F.lag("txn_time").over(w) + F.expr("INTERVAL 60 DAYS")), 1).otherwise(0))
)

sessions_transactions = (
    with_gaps
    .select(
        "id",
        "account_id",
        "txn_type",
        "amount",
        "txn_time",
        "merchant",
        F.sum("new_group_flag").over(w2).alias("session_id"),
    )
)

result = (
    sessions_transactions
    .groupBy(F.date_trunc("MONTH", "txn_time").alias("activity_month"))
    .agg(
        F.count("*").alias("row_count"),
        F.stddev_samp("amount").alias("amount_stddev"),
    )
    .filter(F.col("amount_stddev") >= 0)
)
