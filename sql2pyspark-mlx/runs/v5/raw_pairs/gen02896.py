from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("merchant").orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("merchant").orderBy("amount", "id").rowsBetween(-5, -1)
w3 = Window.partitionBy("merchant").orderBy(F.col("txn_time").cast("bigint")).rangeBetween(-604800, Window.currentRow)
w4 = Window.partitionBy("merchant").orderBy(F.col("amount").desc(), "id")
w5 = Window.partitionBy("merchant").orderBy("running_amount", "id")
w6 = Window.partitionBy("merchant").orderBy("txn_time", "id")
w7 = Window.partitionBy("merchant").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_transactions
    .filter(F.hour("txn_time").between(11, 18))
    .withColumn("amount_rounded", F.round("amount", 0))
    .withColumn("is_flagged_label", F.when(F.col("is_flagged"), "Yes").otherwise("No"))
    .withColumn("merchant_filled", F.coalesce("merchant", F.lit("N/A")))
)

filtered = (
    valid
    .filter(F.col("merchant").isNotNull())
    .select(
        "id",
        "amount",
        "txn_time",
        "merchant",
        "is_flagged",
        "is_flagged_label",
        "merchant_filled",
        F.to_date("txn_time").alias("txn_time_date"),
        F.col("txn_time").cast("date").alias("txn_time_day"),
    )
)

metrics = (
    filtered
    .select(
        "*",
        F.sum("amount").over(w).alias("running_amount"),
        F.avg("amount").over(w2).alias("amount_avg_5_prior"),
        F.max("amount").over(w3).alias("amount_max_7d"),
        F.ntile(5).over(w4).alias("amount_bucket"),
        F.sum("amount").over(w3).alias("amount_sum_7d"),
    )
)

latest = (
    metrics
    .withColumn("rn", F.row_number().over(w5))
    .filter(F.col("rn") == 1)
)

flagged = (
    latest
    .withColumn("is_new_session", F.when(F.lag("txn_time").over(w6).isNull() | (F.col("txn_time") > F.lag("txn_time").over(w6) + F.expr("INTERVAL 90 DAYS")), 1).otherwise(0))
)

sessions_transactions = (
    flagged
    .select(
        "id",
        "amount",
        "txn_time",
        "merchant",
        "is_flagged",
        "is_flagged_label",
        "merchant_filled",
        "txn_time_date",
        "txn_time_day",
        "amount_max_7d",
        "amount_sum_7d",
        F.sum("is_new_session").over(w7).alias("session_id"),
    )
)

result = (
    sessions_transactions
    .groupBy(F.date_trunc("MONTH", "txn_time_date").alias("activity_month"))
    .agg(
        F.size(F.collect_set("merchant")).alias("num_merchants"),
        F.round(F.avg("amount"), 2).alias("avg_amount"),
    )
    .orderBy(F.col("avg_amount").desc())
)
