from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("merchant").orderBy("amount_filled", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("account_id").orderBy(F.col("amount_filled").asc_nulls_last())
w3 = Window.partitionBy("merchant").orderBy("txn_time", "id")
w4 = Window.partitionBy("merchant").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_transactions
    .filter(F.col("merchant").isin(["Starbucks", "Lazada", "7-Eleven"]))
    .select(
        "*",
        F.coalesce("amount", F.lit(0)).alias("amount_filled"),
        F.coalesce("merchant", F.lit("NONE")).alias("resolved_merchant"),
    )
)

scored = (
    base
    .select(
        "id",
        "amount",
        "txn_time",
        "merchant",
        "is_flagged",
        "resolved_merchant",
        F.sum("amount_filled").over(w).alias("cumulative_amount_filled"),
        F.dense_rank().over(w2).alias("amount_filled_dense_rank"),
    )
)

with_prev = scored.withColumn("prev_txn_time", F.lag("txn_time").over(w3))

gaps = (
    with_prev
    .withColumn("new_group_flag", F.when(F.col("prev_txn_time").isNull() | (F.col("txn_time") > F.col("prev_txn_time") + F.expr("INTERVAL 14 DAYS")), 1).otherwise(0))
)

sessionized = gaps.withColumn("session_id", F.sum("new_group_flag").over(w4))

valid = (
    sessionized
    .filter(F.col("txn_time") < "2024-08-01")
    .select(
        "id",
        "amount",
        "txn_time",
        "merchant",
        "is_flagged",
        "resolved_merchant",
        "cumulative_amount_filled",
        "amount_filled_dense_rank",
        "prev_txn_time",
        "new_group_flag",
        "session_id",
        (F.col("cumulative_amount_filled") + F.col("amount")).alias("cumulative_amount_filled_plus_amount"),
        (F.col("amount_filled_dense_rank") + F.col("new_group_flag")).alias("amount_filled_dense_rank_plus_new_group_flag"),
    )
)

result = (
    valid
    .groupBy(
        F.when(F.col("session_id") >= 6, "PREMIUM").otherwise("STANDARD").alias("session_id_tier"),
        F.when(F.col("cumulative_amount_filled_plus_amount") >= 68, "LARGE").otherwise("SMALL").alias("cumulative_amount_filled_plus_amount_tier"),
        F.when(F.col("amount") >= 4100, "PREMIUM").otherwise("STANDARD").alias("amount_tier"),
    )
    .agg(
        F.max("amount_filled_dense_rank_plus_new_group_flag").alias("max_amount_filled_dense_rank_plus_new_group_flag"),
        F.round(F.avg("amount_filled_dense_rank"), 2).alias("avg_amount_filled_dense_rank"),
        F.count(F.when(F.col("merchant") == "Lazada", 1)).alias("lazada_rows"),
    )
)
