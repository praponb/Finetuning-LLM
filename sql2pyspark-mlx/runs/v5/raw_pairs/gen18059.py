from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.partitionBy("account_id").orderBy("amount", "id").rowsBetween(-2, Window.currentRow)
w2 = Window.partitionBy("merchant").orderBy(F.col("amount").desc())
w3 = Window.orderBy(F.col("amount_cume_dist").desc_nulls_last())
w_running = Window.orderBy(F.col("txn_time").cast("bigint")).rangeBetween(-604800, Window.currentRow)

with_windows = (
    df_transactions
    .withColumn("last3_count_amount", F.count("amount").over(w_recent))
    .withColumn("last3_min_amount", F.min("amount").over(w_recent))
    .withColumn("last3_avg_amount", F.avg("amount").over(w_recent))
    .withColumn("amount_cume_dist", F.cume_dist().over(w2))
)

matched_keys = (
    with_windows
    .select("id")
    .exceptAll(
        df_transactions
        .filter(F.col("account_id").isin([4, 22, 28, 34]))
        .select("id")
    )
)

verified = (
    with_windows
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

scored = (
    verified
    .filter(F.col("last3_count_amount") < 6)
    .select(
        "merchant",
        "is_flagged",
        F.rank().over(w3).alias("amount_cume_dist_rank"),
        F.count("amount").over(w_running).alias("amount_count_7d"),
    )
)

filtered = (
    scored
    .filter(F.col("amount_cume_dist_rank") < 8)
    .withColumn("is_flagged_label", F.when(F.col("is_flagged"), "Yes").otherwise("No"))
)

base = (
    filtered
    .filter(F.col("merchant").isNotNull())
    .select(
        "merchant",
        "is_flagged",
        "amount_count_7d",
        "is_flagged_label",
        (F.col("amount_cume_dist_rank") + F.col("amount_count_7d")).alias("amount_cume_dist_rank_plus_amount_count_7d"),
        F.coalesce("merchant", F.lit("N/A")).alias("merchant_clean"),
    )
)

result = (
    base
    .select(
        "merchant",
        "is_flagged",
        "amount_count_7d",
        "is_flagged_label",
        "amount_cume_dist_rank_plus_amount_count_7d",
        F.when(F.col("merchant").rlike("^(Starbucks|Amazon)$"), "PRIORITY").when(F.col("merchant").rlike("^(Grab|7-Eleven)$"), "OTHER_ZONE").otherwise("CORE").alias("merchant_zone"),
    )
    .orderBy("amount_count_7d", F.col("merchant").desc())
)
