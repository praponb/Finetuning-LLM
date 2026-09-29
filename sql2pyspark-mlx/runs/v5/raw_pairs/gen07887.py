from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("merchant", "txn_type").orderBy("amount_whole", "id")
w2 = Window.orderBy(F.col("txn_time").cast("bigint")).rangeBetween(-2592000, Window.currentRow)
w3 = Window.orderBy("prev_amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_recent = Window.partitionBy("merchant").orderBy(F.col("txn_time").cast("bigint")).rangeBetween(-604800, Window.currentRow)
w5 = Window.partitionBy("account_id").orderBy("amount_whole", "id")
w6 = Window.partitionBy("account_id", "merchant").orderBy("next_amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy("txn_type").orderBy("txn_time", "id")
w8 = Window.partitionBy("txn_type").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_transactions
    .filter((F.col("txn_time") >= F.lit("2023-10-28 03:00:00").cast("timestamp")) & F.col("merchant").isNotNull())
    .withColumn("merchant_group", F.when(F.col("merchant").isin(["Starbucks", "Shell"]), "Core").otherwise("Other"))
    .withColumn("amount_whole", F.ceil("amount"))
    .withColumn("resolved_merchant", F.coalesce("merchant", F.lit("N/A")))
)

with_windows = (
    cleaned
    .withColumn("next_amount", F.lead("amount").over(w))
    .withColumn("prev_amount", F.lag("amount").over(w))
    .withColumn("prev_amount_whole", F.lag("amount_whole").over(w))
    .withColumn("seq_no", F.row_number().over(w))
    .withColumn("amount_count_30d", F.count("amount").over(w2))
)

enriched = (
    with_windows
    .withColumn("last_known_amount_whole", F.last("amount_whole", ignorenulls=True).over(w3))
    .withColumn("prev_amount_whole_count_7d", F.count("prev_amount_whole").over(w_recent))
    .withColumn("prev_next_amount", F.lag("next_amount", 2).over(w5))
    .withColumn("cumulative_amount_whole", F.sum("amount_whole").over(w6))
)

with_prev = enriched.withColumn("prev_txn_time", F.lag("txn_time").over(w7))

with_gaps = (
    with_prev
    .withColumn("is_new_burst", F.when(F.col("prev_txn_time").isNull() | (F.col("txn_time") > F.col("prev_txn_time") + F.expr("INTERVAL 60 DAYS")), 1).otherwise(0))
)

sessions_transactions = with_gaps.withColumn("burst_id", F.sum("is_new_burst").over(w8))

result = (
    sessions_transactions
    .groupBy(
        F.when(F.col("amount") >= 4100, "LARGE").otherwise("SMALL").alias("amount_tier"),
        F.date_trunc("MONTH", "txn_time").alias("activity_month"),
        F.when(F.col("amount_whole") >= 78, "LARGE").otherwise("SMALL").alias("amount_whole_tier"),
    )
    .agg(
        F.count_if(F.col("txn_type") == "REFUND").alias("refund_events"),
        F.round(F.avg("is_new_burst"), 2).alias("avg_is_new_burst"),
    )
    .filter(F.col("avg_is_new_burst") >= 0)
    .orderBy(F.col("activity_month").desc())
)
