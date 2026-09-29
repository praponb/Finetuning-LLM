from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.partitionBy("merchant").orderBy(F.col("is_flagged_flag").desc(), "id")
w2 = Window.partitionBy("merchant")
w3 = Window.partitionBy("account_id").orderBy("is_flagged_flag_bucket", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("merchant").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("account_id", "txn_type").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("account_id").orderBy(F.col("txn_time").cast("bigint")).rangeBetween(-2592000, Window.currentRow)

cleaned = (
    df_transactions
    .filter(F.col("merchant").isNotNull())
    .withColumn("is_flagged_flag", F.col("is_flagged").cast("int"))
)

enriched = (
    cleaned
    .withColumn("is_flagged_flag_bucket", F.ntile(4).over(w_recent))
    .withColumn("merchant_min_amount", F.min("amount").over(w2))
)

enriched_transactions = (
    enriched
    .select(
        "id",
        "account_id",
        "txn_type",
        "txn_time",
        "is_flagged",
        "is_flagged_flag_bucket",
        "merchant_min_amount",
        F.first("amount", ignorenulls=True).over(w3).alias("first_known_amount"),
        F.first("merchant", ignorenulls=True).over(w4).alias("first_known_merchant"),
    )
)

scored = (
    enriched_transactions
    .filter(F.col("account_id").isin([15, 19]))
    .select(
        "id",
        "account_id",
        "txn_type",
        "is_flagged",
        "is_flagged_flag_bucket",
        "first_known_amount",
        F.count("is_flagged_flag_bucket").over(w5).alias("running_count_is_flagged_flag_bucket"),
        F.min("is_flagged_flag_bucket").over(w5).alias("running_min_is_flagged_flag_bucket"),
        F.avg("is_flagged_flag_bucket").over(w5).alias("running_avg_is_flagged_flag_bucket"),
        F.max("is_flagged_flag_bucket").over(w5).alias("running_max_is_flagged_flag_bucket"),
        F.max("merchant_min_amount").over(w6).alias("merchant_min_amount_max_30d"),
    )
)

grouped = (
    scored
    .groupBy("txn_type")
    .agg(
        F.round(F.sum("running_max_is_flagged_flag_bucket") / F.nullif(F.count(F.when(F.col("txn_type") == "DEBIT", 1)), F.lit(0)), 2).alias("running_max_is_flagged_flag_bucket_per_debit"),
        F.round(F.sum("running_max_is_flagged_flag_bucket") / F.nullif(F.count(F.when(F.col("txn_type") == "DEBIT", 1)), F.lit(0)), 2).alias("running_max_is_flagged_flag_bucket_per_debit_2"),
        F.percentile_approx("first_known_amount", 0.9).alias("first_known_amount_p90"),
        F.percentile_approx("running_max_is_flagged_flag_bucket", [0.25, 0.5, 0.75]).alias("running_max_is_flagged_flag_bucket_quartiles"),
        F.count("*").alias("event_count"),
        F.round(F.sum("running_min_is_flagged_flag_bucket") / F.nullif(F.count(F.when(F.col("txn_type") == "REFUND", 1)), F.lit(0)), 2).alias("running_min_is_flagged_flag_bucket_per_refund"),
    )
    .filter(F.col("running_max_is_flagged_flag_bucket_per_debit_2") > 0)
)

result = (
    grouped
    .select(
        "txn_type",
        "running_max_is_flagged_flag_bucket_quartiles",
        "running_min_is_flagged_flag_bucket_per_refund",
    )
    .orderBy(F.col("running_min_is_flagged_flag_bucket_per_refund").desc())
)
