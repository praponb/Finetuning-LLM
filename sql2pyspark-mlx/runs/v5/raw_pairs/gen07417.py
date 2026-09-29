from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.partitionBy("merchant").orderBy("txn_time_period", "id").rowsBetween(-3, Window.currentRow)
w2 = Window.partitionBy("merchant").orderBy("txn_time_period", "id")
w3 = Window.partitionBy("merchant").orderBy("txn_time_period", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_transactions
    .filter((F.col("txn_time") >= F.lit("2023-06-21 02:00:00").cast("timestamp")) & F.col("merchant").isNotNull())
    .select(
        "id",
        "account_id",
        "txn_type",
        "amount",
        "merchant",
        "is_flagged",
        F.date_trunc("WEEK", "txn_time").alias("txn_time_period"),
        F.coalesce("merchant", F.lit("DIRECT")).alias("merchant_filled"),
    )
)

metrics = (
    base
    .filter(F.col("txn_type") == "FEE")
    .withColumn("last4_count_amount", F.count("amount").over(w_part))
    .withColumn("last4_avg_amount", F.avg("amount").over(w_part))
    .withColumn("last4_max_amount", F.max("amount").over(w_part))
)

with_dims = (
    metrics
    .alias("a")
    .join(df_accounts.alias("b"), F.col("a.account_id") == F.col("b.id"), "inner")
    .select("a.*", F.col("b.status"), F.col("b.account_type"))
)

prev_events = with_dims.withColumn("prev_txn_time_period", F.lag("txn_time_period").over(w2))

with_gaps = (
    prev_events
    .withColumn("new_group_flag", F.when(F.col("prev_txn_time_period").isNull() | (F.col("txn_time_period") > F.col("prev_txn_time_period") + F.expr("INTERVAL 14 DAYS")), 1).otherwise(0))
)

islands = (
    with_gaps
    .select(
        "id",
        "amount",
        "merchant",
        "txn_time_period",
        "last4_count_amount",
        "last4_avg_amount",
        "account_type",
        F.sum("new_group_flag").over(w3).alias("session_no"),
    )
)

result = (
    islands
    .groupBy("merchant")
    .pivot("account_type", ["CHECKING", "LOAN", "CREDIT"])
    .agg(F.avg("last4_avg_amount"))
)
