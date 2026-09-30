from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("merchant").orderBy(F.col("amount").desc_nulls_last())
w2 = Window.partitionBy("merchant").orderBy(F.col("txn_time").cast("bigint")).rangeBetween(-604800, Window.currentRow)

base = (
    df_transactions
    .filter((F.col("txn_time") >= F.lit("2023-11-02 06:00:00").cast("timestamp")) & (~F.col("merchant").eqNullSafe(F.lit("7-Eleven"))))
    .withColumn("is_flagged_flag", F.col("is_flagged").cast("int"))
    .withColumn("merchant_lower", F.lower("merchant"))
    .withColumn("merchant_clean", F.coalesce("merchant", F.lit("N/A")))
)

kept_keys = (
    base
    .select("merchant")
    .subtract(
        df_transactions
        .filter(F.col("account_id").isin([39, 40]))
        .select("merchant")
    )
)

reconciled = (
    base
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.merchant") == F.col("k.merchant")), "inner")
    .select("s.*")
)

scored = (
    reconciled
    .select(
        "txn_type",
        "txn_time",
        F.cume_dist().over(w).alias("amount_cume_dist"),
        F.sum("is_flagged_flag").over(w2).alias("is_flagged_flag_sum_7d"),
    )
)

filtered = (
    scored
    .filter(F.col("txn_time") >= F.lit("2023-05-06 12:00:00").cast("timestamp"))
    .select(
        "*",
        F.date_format("txn_time", "yyyy-MM").alias("txn_time_label"),
        F.col("amount_cume_dist").cast("int").alias("amount_cume_dist_int"),
    )
)

valid = (
    filtered
    .filter((F.col("txn_time") >= F.to_date(F.lit("2023-05-02"))) & (F.col("txn_type") != "REFUND"))
    .withColumn("greatest_is_flagged_flag_sum_7d_amount_cume_dist", F.greatest("is_flagged_flag_sum_7d", "amount_cume_dist"))
)

totals = (
    valid
    .rollup(
        F.when(F.col("greatest_is_flagged_flag_sum_7d_amount_cume_dist") >= 72, "HIGH").otherwise("LOW").alias("greatest_is_flagged_flag_sum_7d_amount_cume_dist_tier"),
        "txn_type",
    )
    .agg(
        F.countDistinct("txn_type").alias("distinct_txn_types"),
        ((F.unix_timestamp(F.max("txn_time")) - F.unix_timestamp(F.min("txn_time"))) / 60).alias("duration_min"),
        F.percentile_approx("greatest_is_flagged_flag_sum_7d_amount_cume_dist", 0.95).alias("greatest_is_flagged_flag_sum_7d_amount_cume_dist_p95"),
        F.count_if(F.col("txn_type") == "REFUND").alias("refund_events"),
    )
    .filter(F.col("duration_min") > 0)
)

result = (
    totals
    .select(
        "greatest_is_flagged_flag_sum_7d_amount_cume_dist_tier",
        "txn_type",
        "distinct_txn_types",
        "duration_min",
        "greatest_is_flagged_flag_sum_7d_amount_cume_dist_p95",
    )
    .orderBy("distinct_txn_types")
)
