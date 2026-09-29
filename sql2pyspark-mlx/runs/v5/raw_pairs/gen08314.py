from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("txn_type").orderBy("amount", "id")
w2 = Window.orderBy(F.col("amount").desc(), "id")
w3 = Window.partitionBy("account_id")
w4 = Window.partitionBy("account_id").orderBy("amount", "id")
w5 = Window.partitionBy("account_id").orderBy(F.col("amount_bucket").desc(), "id")
w6 = Window.orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy("account_id").orderBy("txn_time", "id")
w8 = Window.partitionBy("account_id").orderBy(F.col("amount_bucket").desc_nulls_last())
w9 = Window.partitionBy("account_id").orderBy(F.col("last_known_amount_bucket").desc_nulls_first(), "id")

filtered = (
    df_transactions
    .filter((F.col("txn_time") >= F.to_date(F.lit("2023-06-15"))) & (~F.col("merchant").eqNullSafe(F.lit("Lazada"))))
    .select(
        "id",
        "account_id",
        "txn_type",
        "amount",
        "txn_time",
        "is_flagged",
        F.lower("txn_type").alias("txn_type_lower"),
        F.coalesce("merchant", F.lit("N/A")).alias("merchant_filled"),
    )
)

ranked = (
    filtered
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
    .select("id", "account_id", "amount", "txn_time", "is_flagged", "txn_type_lower")
)

enriched = (
    ranked
    .withColumn("amount_bucket", F.ntile(10).over(w2))
    .withColumn("amount_share", F.round((F.col("amount") / F.sum("amount").over(w3)), 4))
    .withColumn("next_amount", F.lead("amount", 1).over(w4))
)

latest = (
    enriched
    .withColumn("rn", F.row_number().over(w5))
    .filter(F.col("rn") == 1)
    .select(
        "id",
        "account_id",
        "txn_time",
        "is_flagged",
        "txn_type_lower",
        "amount_bucket",
    )
)

with_windows = (
    latest
    .select(
        "*",
        F.last("amount_bucket", ignorenulls=True).over(w6).alias("last_known_amount_bucket"),
        F.lag("amount_bucket", 2).over(w7).alias("prev_amount_bucket"),
        F.cume_dist().over(w8).alias("amount_bucket_cume_dist"),
    )
)

deduped = (
    with_windows
    .withColumn("rank_in_group", F.row_number().over(w9))
    .filter(F.col("rank_in_group") == 1)
    .select(
        "id",
        "account_id",
        "txn_time",
        "is_flagged",
        "txn_type_lower",
        "amount_bucket",
        "last_known_amount_bucket",
        "prev_amount_bucket",
    )
)

summary = (
    deduped
    .groupBy(
        F.when(F.col("last_known_amount_bucket") >= 8, "HIGH").otherwise("LOW").alias("last_known_amount_bucket_tier"),
    )
    .agg(
        F.min("last_known_amount_bucket").alias("min_last_known_amount_bucket"),
        F.max("txn_time").alias("last_txn_time"),
        F.round(F.avg("amount_bucket"), 2).alias("avg_amount_bucket"),
    )
)

result = (
    summary
    .select(
        "last_known_amount_bucket_tier",
        "min_last_known_amount_bucket",
        "avg_amount_bucket",
    )
    .orderBy("last_known_amount_bucket_tier")
)
