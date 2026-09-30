from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_id")
w2 = Window.partitionBy("account_id", "merchant").orderBy("is_grab_plus_amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("merchant")
w4 = Window.orderBy("is_grab", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("account_id", "txn_type").orderBy(F.col("is_grab_plus_amount").desc(), "id")
w_grp = Window.orderBy(F.col("is_grab").desc(), "id")
w7 = Window.partitionBy("txn_type").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.partitionBy("quarter_start").orderBy(F.col("min_first_known_is_grab").desc())

valid = (
    df_transactions
    .filter(F.col("txn_time") >= F.to_date(F.lit("2023-11-27")))
    .select(
        "*",
        F.when(F.col("merchant") == "Grab", 1).otherwise(0).alias("is_grab"),
        F.coalesce("merchant", F.lit("N/A")).alias("merchant_clean"),
    )
)

base = (
    valid
    .filter((F.col("txn_time") > F.to_date(F.lit("2023-12-16"))) & F.col("merchant").isNotNull() & F.col("is_flagged"))
    .withColumn("is_grab_plus_amount", (F.col("is_grab") + F.col("amount")))
)

with_windows = (
    base
    .select(
        "*",
        F.min("is_grab").over(w).alias("account_id_min_is_grab"),
        F.sum("is_grab_plus_amount").over(w2).alias("running_is_grab_plus_amount"),
    )
)

joined = (
    with_windows
    .alias("src")
    .join(df_accounts.alias("d"), F.col("src.account_id") == F.col("d.id"), "left")
    .select("src.*", F.col("d.opened_date"))
)

metrics = (
    joined
    .withColumn("amount_share", F.round((F.col("amount") / F.sum("amount").over(w3)), 4))
    .withColumn("cumulative_account_id_min_is_grab", F.sum("account_id_min_is_grab").over(w4))
    .withColumn("is_grab_plus_amount_bucket", F.ntile(5).over(w5))
    .withColumn("is_grab_bucket", F.ntile(5).over(w_grp))
    .withColumn("first_known_is_grab", F.first("is_grab", ignorenulls=True).over(w7))
)

grouped = (
    metrics
    .groupBy(
        F.date_trunc("QUARTER", "txn_time").alias("quarter_start"),
        F.when(F.col("is_grab_bucket") >= 9, "PREMIUM").otherwise("STANDARD").alias("is_grab_bucket_tier"),
        "account_id",
    )
    .agg(
        F.min("first_known_is_grab").alias("min_first_known_is_grab"),
        F.max("txn_time").alias("ended_at"),
        F.sum(F.when(F.col("txn_type") == "REFUND", F.col("cumulative_account_id_min_is_grab")).otherwise(0)).alias("refund_cumulative_account_id_min_is_grab"),
        F.percentile_approx("amount_share", [0.25, 0.5, 0.75]).alias("amount_share_quartiles"),
    )
    .withColumn("min_first_known_is_grab_percent_rank", F.percent_rank().over(w8))
)

result = (
    grouped
    .select(
        "quarter_start",
        "is_grab_bucket_tier",
        "account_id",
        "ended_at",
        "refund_cumulative_account_id_min_is_grab",
        "min_first_known_is_grab_percent_rank",
    )
    .orderBy(F.col("ended_at").desc_nulls_last())
)
