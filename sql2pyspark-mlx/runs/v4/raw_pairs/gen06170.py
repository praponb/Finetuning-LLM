from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_id").orderBy("amount_rounded")
w2 = Window.partitionBy("txn_type", "account_id").orderBy(F.col("amount_rounded").desc(), "id")
w_part = Window.partitionBy("account_id").orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("merchant").orderBy("amount", "id")
w_part_2 = Window.orderBy("amount_scaled", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("account_id", "txn_type").orderBy("running_amount", "id")
w7 = Window.partitionBy("merchant", "txn_type").orderBy(F.col("running_amount_2").desc(), "id")
w8 = Window.partitionBy("txn_type").orderBy("amount_rounded", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w9 = Window.partitionBy("account_id")

base = (
    df_transactions
    .filter(F.col("merchant").isNotNull())
    .select(
        "id",
        "account_id",
        "txn_type",
        "amount",
        "merchant",
        "is_flagged",
        F.round("amount", 2).alias("amount_rounded"),
        F.coalesce("merchant", F.lit("N/A")).alias("merchant_clean"),
    )
)

ranked = (
    base
    .withColumn("rank_in_group", F.rank().over(w))
    .filter(F.col("rank_in_group") == 1)
    .drop("rank_in_group")
)

cleaned = (
    ranked
    .filter(F.col("merchant").isNotNull() & (F.col("amount_rounded") <= 31))
    .select("*", F.round((F.col("amount") * 0.9), 2).alias("amount_scaled"))
)

scored = (
    cleaned
    .select(
        "*",
        F.row_number().over(w2).alias("seq_no"),
        F.sum("amount").over(w_part).alias("running_amount"),
        F.lag("amount_rounded", 1, 0).over(w4).alias("prev_amount_rounded"),
        F.sum("amount").over(w_part_2).alias("running_amount_2"),
    )
)

with_windows = (
    scored
    .withColumn("seq_no_2", F.row_number().over(w6))
    .withColumn("running_amount_2_bucket", F.ntile(3).over(w7))
    .withColumn("last_known_amount_scaled", F.last("amount_scaled", ignorenulls=True).over(w8))
    .withColumn("account_id_min_running_amount", F.min("running_amount").over(w9))
)

stats = (
    with_windows
    .cube(
        F.when(F.col("running_amount") >= 21638, "LARGE").otherwise("SMALL").alias("running_amount_tier"),
        F.when(F.col("running_amount_2_bucket") >= 8, "HIGH").otherwise("LOW").alias("running_amount_2_bucket_tier"),
    )
    .agg(
        F.count(F.when(F.col("merchant") == "Grab", 1)).alias("grab_count"),
        F.round(F.avg("amount"), 2).alias("avg_amount"),
        F.count(F.when(F.col("merchant") == "Starbucks", 1)).alias("starbucks_count"),
        F.countDistinct("merchant").alias("distinct_merchants"),
        F.percentile_approx("running_amount_2_bucket", [0.25, 0.5, 0.75]).alias("running_amount_2_bucket_quartiles"),
    )
)

result = (
    stats
    .select(
        "running_amount_tier",
        "running_amount_2_bucket_tier",
        "grab_count",
        "starbucks_count",
        "distinct_merchants",
        "running_amount_2_bucket_quartiles",
    )
    .orderBy(F.col("running_amount_2_bucket_tier").desc())
)
