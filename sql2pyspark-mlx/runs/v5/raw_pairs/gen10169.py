from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("txn_type").orderBy("txn_time", "id")
w2 = Window.partitionBy("txn_type").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("txn_type").orderBy("session_no")

valid = (
    df_transactions
    .filter(F.col("merchant").isNotNull() & (F.col("txn_time") < "2025-05-01"))
    .withColumn("days_since_txn_time", F.datediff(F.current_date(), "txn_time"))
    .withColumn("days_since_txn_time_2", F.datediff(F.current_date(), "txn_time"))
)

flagged = (
    valid
    .withColumn("is_new_burst", F.when(F.col("txn_time") - F.lag("txn_time").over(w) > F.expr("INTERVAL 30 DAYS"), 1).otherwise(0))
)

islands = (
    flagged
    .select(
        "id",
        "txn_type",
        "txn_time",
        "merchant",
        "is_flagged",
        "days_since_txn_time_2",
        F.sum("is_new_burst").over(w2).alias("session_no"),
    )
)

filtered = (
    islands
    .filter(F.col("merchant").isNotNull())
    .withColumn("txn_time_day", F.col("txn_time").cast("date"))
    .withColumn("days_since_txn_time_2_minus_session_no", (F.col("days_since_txn_time_2") - F.col("session_no")))
)

deduped = (
    filtered
    .withColumn("latest_rank", F.rank().over(w3))
    .filter(F.col("latest_rank") == 1)
)

result = (
    deduped
    .groupBy(F.date_trunc("MONTH", "txn_time").alias("activity_month"))
    .agg(
        F.avg(F.when(F.col("merchant") == "Shell", F.col("session_no"))).alias("shell_avg_session_no"),
        F.min("txn_time").alias("started_at"),
        ((F.unix_timestamp(F.max("txn_time")) - F.unix_timestamp(F.min("txn_time"))) / 60).alias("duration_min"),
        F.min("txn_time").alias("started_at_2"),
        F.percentile_approx("days_since_txn_time_2_minus_session_no", [0.25, 0.5, 0.75]).alias("days_since_txn_time_2_minus_session_no_quartiles"),
    )
)
