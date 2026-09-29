from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("merchant").orderBy("txn_time", "id")
w2 = Window.partitionBy("merchant").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_transactions
    .filter(F.col("merchant").isNotNull() & (~F.col("merchant").eqNullSafe(F.lit("Shell"))))
    .select(
        "id",
        "account_id",
        "amount",
        "txn_time",
        "merchant",
        "is_flagged",
        F.concat_ws("-", "merchant", "txn_type").alias("merchant_txn_type_key"),
        F.coalesce("merchant", F.lit("UNKNOWN")).alias("merchant_clean"),
    )
)

with_prev = filtered.withColumn("prev_txn_time", F.lag("txn_time").over(w))

gaps = (
    with_prev
    .withColumn("new_group_flag", F.when(F.col("prev_txn_time").isNull() | (F.col("txn_time") > F.col("prev_txn_time") + F.expr("INTERVAL 14 DAYS")), 1).otherwise(0))
)

islands = (
    gaps
    .select(
        "id",
        "amount",
        "txn_time",
        "merchant",
        "is_flagged",
        "merchant_txn_type_key",
        "merchant_clean",
        F.sum("new_group_flag").over(w2).alias("island_id"),
    )
)

result = (
    islands
    .groupBy("merchant")
    .agg(
        F.min("island_id").alias("min_island_id"),
        F.count("*").alias("row_count"),
    )
)
