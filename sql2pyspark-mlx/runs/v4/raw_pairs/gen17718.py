from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan", "status").orderBy(F.col("is_cancelled").desc_nulls_first())
w_hist = Window.partitionBy("plan")
w3 = Window.partitionBy("plan", "status")
w_part = Window.partitionBy("plan").orderBy("is_cancelled_dense_rank")

remaining_keys = (
    df_subscriptions
    .select("id")
    .intersectAll(
        df_subscriptions
        .filter(F.col("status").isin(["PAUSED", "ACTIVE"]))
        .select("id")
    )
)

reconciled = (
    df_subscriptions
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

base = (
    reconciled
    .filter((F.col("start_date") > F.lit("2023-07-17").cast("date")) & F.col("customer_id").isNotNull())
    .select(
        "id",
        "plan",
        "start_date",
        "end_date",
        "monthly_fee",
        "status",
        F.quarter("end_date").alias("end_date_quarter"),
        F.when(F.col("status") == "CANCELLED", 1).otherwise(0).alias("is_cancelled"),
    )
)

metrics = (
    base
    .select(
        "*",
        F.dense_rank().over(w).alias("is_cancelled_dense_rank"),
        F.min("monthly_fee").over(w_hist).alias("plan_min_monthly_fee"),
    )
)

metrics_subscriptions = (
    metrics
    .withColumn("monthly_fee_share", F.round((F.col("monthly_fee") / F.sum("monthly_fee").over(w3)), 4))
    .withColumn("is_cancelled_dense_rank_percent_rank", F.percent_rank().over(w_part))
    .withColumn("is_cancelled_dense_rank_percent_rank_2", F.percent_rank().over(w_part))
)

matched_keys = (
    metrics_subscriptions
    .select("id")
    .intersectAll(
        df_subscriptions
        .filter(F.month("start_date").isin([2, 5, 11]))
        .select("id")
    )
)

verified = (
    metrics_subscriptions
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    verified
    .withColumn("is_cancelled_dense_rank_percent_rank_2_tier", F.when(F.col("is_cancelled_dense_rank_percent_rank_2") >= 0.64, "PREMIUM").otherwise("STANDARD"))
    .groupingSets(
        [["is_cancelled_dense_rank_percent_rank_2_tier", "plan"], ["is_cancelled_dense_rank_percent_rank_2_tier"]],
        "is_cancelled_dense_rank_percent_rank_2_tier",
        "plan",
    )
    .agg(
        F.count("*").alias("row_count"),
        F.max(F.when(F.col("plan") == "basic", F.col("monthly_fee"))).alias("basic_max_monthly_fee"),
    )
    .orderBy(F.col("plan").desc())
)
