from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("merchant").orderBy("txn_time", "id")
w2 = Window.partitionBy("merchant").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("merchant").orderBy(F.col("refund_count").desc())

valid = (
    df_transactions
    .filter((F.col("txn_time") > F.lit("2023-11-08").cast("date")) & F.col("txn_type").isin(["FEE", "DEBIT"]))
    .withColumn("amount_int", F.col("amount").cast("int"))
    .withColumn("merchant_filled", F.coalesce("merchant", F.lit("OTHER")))
)

with_prev = valid.withColumn("prev_txn_time", F.lag("txn_time").over(w))

gaps = (
    with_prev
    .withColumn("is_new_session", F.when(F.col("prev_txn_time").isNull() | (F.col("txn_time") > F.col("prev_txn_time") + F.expr("INTERVAL 60 DAYS")), 1).otherwise(0))
)

islands = (
    gaps
    .select(
        "id",
        "txn_type",
        "amount",
        "txn_time",
        "merchant",
        "is_flagged",
        F.sum("is_new_session").over(w2).alias("island_id"),
    )
)

result = (
    islands
    .groupBy("merchant", F.date_trunc("YEAR", "txn_time").alias("year_start"))
    .agg(
        F.count(F.when(F.col("txn_type") == "REFUND", 1)).alias("refund_rows"),
        F.max("txn_time").alias("ended_at"),
        F.count(F.when(F.col("txn_type") == "REFUND", 1)).alias("refund_count"),
        F.min("txn_time").alias("first_txn_time"),
    )
    .filter(F.col("refund_rows") > 1)
    .withColumn("refund_count_rank", F.rank().over(w3))
    .orderBy(F.col("year_start").desc_nulls_last())
)
