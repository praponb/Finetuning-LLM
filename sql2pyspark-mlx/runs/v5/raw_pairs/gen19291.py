from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.partitionBy("plan").orderBy(F.col("monthly_fee").desc(), "id", "subscription_id")
w_part = Window.partitionBy("status").orderBy("subscription_monthly_fee", "id", "subscription_id")
w_running = Window.partitionBy("plan").orderBy(F.col("subscription_monthly_fee").desc(), "id", "subscription_id")
w4 = Window.orderBy("subscription_id", "id", "subscription_id").rowsBetween(-2, -1)
w5 = Window.partitionBy("status").orderBy("subscription_id", "id", "subscription_id")
w6 = Window.partitionBy("plan")
w7 = Window.partitionBy("status")

with_window = (
    df_subscriptions
    .alias("e")
    .join(df_subscriptions.alias("sub"), (F.col("e.customer_id") == F.col("sub.customer_id")) & (F.col("e.start_date") >= F.col("sub.start_date")) & (F.col("e.start_date") < F.col("sub.end_date") + F.expr("INTERVAL 14 DAYS")), "inner")
    .select(
        "e.*",
        F.col("sub.id").alias("subscription_id"),
        F.col("sub.plan").alias("subscription_plan"),
        F.col("sub.monthly_fee").alias("subscription_monthly_fee"),
    )
)

metrics = (
    with_window
    .filter(F.year("end_date") == 2025)
    .select(
        "*",
        F.ntile(4).over(w_grp).alias("monthly_fee_bucket"),
        F.lead("subscription_monthly_fee", 2).over(w_part).alias("next_subscription_monthly_fee"),
        F.ntile(10).over(w_running).alias("subscription_monthly_fee_bucket"),
        F.avg("subscription_monthly_fee").over(w4).alias("subscription_monthly_fee_avg_2_prior"),
    )
)

enriched = (
    metrics
    .withColumn("prev_subscription_id", F.lag("subscription_id").over(w5))
    .withColumn("prev_next_subscription_monthly_fee", F.lag("next_subscription_monthly_fee").over(w5))
    .withColumn("row_seq", F.row_number().over(w5))
)

matched_keys = (
    enriched
    .select("customer_id", "status")
    .subtract(
        df_subscriptions
        .filter(F.col("customer_id").isin([18, 32, 34, 37]))
        .select("customer_id", "status")
    )
)

reconciled = (
    enriched
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.customer_id") == F.col("k.customer_id")) & (F.col("s.status") == F.col("k.status")), "inner")
    .select("s.*")
)

with_windows = (
    reconciled
    .select(
        "start_date",
        "status",
        "subscription_id",
        "subscription_monthly_fee_bucket",
        "subscription_monthly_fee_avg_2_prior",
        "row_seq",
        F.round((F.col("monthly_fee") / F.sum("monthly_fee").over(w6)), 4).alias("monthly_fee_share"),
        F.round((F.col("next_subscription_monthly_fee") / F.sum("next_subscription_monthly_fee").over(w7)), 4).alias("next_subscription_monthly_fee_share"),
        F.round((F.col("subscription_monthly_fee_bucket") / F.sum("subscription_monthly_fee_bucket").over(w6)), 4).alias("subscription_monthly_fee_bucket_share"),
    )
)

result = (
    with_windows
    .groupBy("status")
    .agg(
        F.count("*").alias("event_count"),
        F.count("*").alias("row_count"),
        F.percentile_approx("subscription_monthly_fee_bucket", [0.25, 0.5, 0.75]).alias("subscription_monthly_fee_bucket_quartiles"),
    )
    .orderBy(F.col("event_count").desc_nulls_last())
)
