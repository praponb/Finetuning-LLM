from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("txn_type").orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_hist = Window.orderBy("running_min_amount", "id").rowsBetween(-6, Window.currentRow)

filtered = (
    df_transactions
    .filter(F.col("merchant").isNotNull())
    .select(
        "id",
        "txn_type",
        "amount",
        "txn_time",
        "is_flagged",
        F.upper("merchant").alias("merchant_upper"),
    )
)

scored = (
    filtered
    .filter(F.col("txn_time") < "2025-04-18")
    .select(
        "id",
        "is_flagged",
        "merchant_upper",
        F.count("amount").over(w).alias("running_count_amount"),
        F.max("amount").over(w).alias("running_max_amount"),
        F.avg("amount").over(w).alias("running_avg_amount"),
        F.min("amount").over(w).alias("running_min_amount"),
        F.first("amount", ignorenulls=True).over(w).alias("first_known_amount"),
    )
)

with_windows = (
    scored
    .withColumn("last7_count_running_min_amount", F.count("running_min_amount").over(w_hist))
    .withColumn("last7_max_running_min_amount", F.max("running_min_amount").over(w_hist))
    .withColumn("last7_min_running_min_amount", F.min("running_min_amount").over(w_hist))
)

matched_keys = (
    with_windows
    .select("id")
    .subtract(
        df_transactions
        .filter(F.col("account_id").isin([5, 6, 32]))
        .select("id")
    )
)

verified = (
    with_windows
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    verified
    .cube(
        F.when(F.col("last7_max_running_min_amount") >= 4200, "LARGE").otherwise("SMALL").alias("last7_max_running_min_amount_tier"),
    )
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.percentile_approx("running_min_amount", [0.25, 0.5, 0.75]).alias("running_min_amount_quartiles"),
        F.round(F.avg("running_max_amount"), 2).alias("avg_running_max_amount"),
    )
    .filter(F.col("avg_running_max_amount") >= 0)
    .orderBy("last7_max_running_min_amount_tier")
)
