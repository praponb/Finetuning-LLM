from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("merchant")
w2 = Window.partitionBy("account_id")
w3 = Window.orderBy(F.col("merchant_stddev_samp_is_flagged_flag").asc_nulls_last())
w4 = Window.orderBy(F.col("txn_time").cast("bigint")).rangeBetween(-2592000, Window.currentRow)

base = (
    df_transactions
    .filter(F.col("merchant").isNotNull() & F.col("merchant").isin(["Grab", "7-Eleven"]))
    .select("*", F.trim("merchant").alias("clean_merchant"))
)

filtered = (
    base
    .filter(F.col("is_flagged"))
    .select(
        "*",
        F.date_format("txn_time", "yyyy-MM").alias("txn_time_label"),
        F.col("is_flagged").cast("int").alias("is_flagged_flag"),
        F.coalesce("merchant", F.lit("UNKNOWN")).alias("merchant_filled"),
    )
)

remaining_keys = (
    filtered
    .select("txn_type", "merchant")
    .exceptAll(
        df_transactions
        .filter(F.col("amount") < 3400)
        .select("txn_type", "merchant")
    )
)

reconciled = (
    filtered
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.txn_type") == F.col("k.txn_type")) & (F.col("s.merchant") == F.col("k.merchant")), "inner")
    .select("s.*")
)

enriched = (
    reconciled
    .withColumn("merchant_stddev_samp_is_flagged_flag", F.stddev_samp("is_flagged_flag").over(w))
    .withColumn("amount_share", F.round((F.col("amount") / F.sum("amount").over(w)), 4))
    .withColumn("amount_share_2", F.round((F.col("amount") / F.sum("amount").over(w2)), 4))
)

filtered_transactions = (
    enriched
    .filter(F.col("amount_share_2") < 0.65)
    .select(
        "account_id",
        "txn_type",
        "amount",
        "txn_time",
        "merchant",
        "clean_merchant",
        "is_flagged_flag",
        "merchant_filled",
        "merchant_stddev_samp_is_flagged_flag",
        "amount_share",
        "amount_share_2",
        F.when(F.col("merchant").isin(["Starbucks", "Grab"]), "Tier 1").otherwise("Other").alias("merchant_group"),
        F.coalesce("merchant", F.lit("NONE")).alias("merchant_filled_2"),
    )
)

scored = (
    filtered_transactions
    .withColumn("account_id_stddev_samp_amount_share_2", F.stddev_samp("amount_share_2").over(w2))
    .withColumn("merchant_stddev_samp_is_flagged_flag_cume_dist", F.cume_dist().over(w3))
    .withColumn("merchant_stddev_samp_is_flagged_flag_avg_30d", F.avg("merchant_stddev_samp_is_flagged_flag").over(w4))
)

result = (
    scored
    .groupBy(
        F.date_trunc("MONTH", "txn_time").alias("activity_month"),
        F.when(F.col("account_id_stddev_samp_amount_share_2") >= 1, "HIGH").otherwise("LOW").alias("account_id_stddev_samp_amount_share_2_tier"),
    )
    .agg(
        F.sum(F.when(F.col("txn_type") == "REFUND", F.col("merchant_stddev_samp_is_flagged_flag")).otherwise(0)).alias("refund_merchant_stddev_samp_is_flagged_flag"),
        F.max("txn_time").alias("last_seen"),
        F.size(F.collect_set("merchant")).alias("num_merchants"),
    )
    .orderBy(F.col("refund_merchant_stddev_samp_is_flagged_flag").asc_nulls_last())
)
