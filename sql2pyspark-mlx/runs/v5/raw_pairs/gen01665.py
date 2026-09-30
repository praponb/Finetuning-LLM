from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.partitionBy("k").orderBy("subscription_id", "id", "k", "subscription_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("plan", "customer_id").orderBy(F.col("monthly_fee").desc(), "id", "k", "subscription_id")
w3 = Window.partitionBy("plan").orderBy("created_at", "id", "k", "subscription_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("k", "plan").orderBy("monthly_fee_bucket")
w5 = Window.partitionBy("k")
w6 = Window.partitionBy("k").orderBy("running_subscription_id", "id", "k", "subscription_id").rowsBetween(-5, Window.currentRow)
w_hist = Window.partitionBy("plan", "k")
w8 = Window.orderBy("monthly_fee_bucket", "id", "k", "subscription_id")
w9 = Window.partitionBy("plan", "k").orderBy("running_subscription_id", "id", "k", "subscription_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w10 = Window.partitionBy("plan").orderBy(F.col("created_at").cast("bigint")).rangeBetween(-7200, Window.currentRow)
w11 = Window.partitionBy("plan").orderBy("created_at", "id", "k", "subscription_id")

flattened = (
    df_carts
    .select(
        "id",
        "customer_id",
        "created_at",
        "coupon_codes",
        F.explode("attributes").alias("k", "v"),
    )
)

attributed = (
    flattened
    .alias("e")
    .join(df_subscriptions.alias("sub"), (F.col("e.customer_id") == F.col("sub.customer_id")) & (F.col("e.created_at") >= F.col("sub.start_date")) & (F.col("e.created_at") < F.col("sub.end_date") + F.expr("INTERVAL 14 DAYS")), "inner")
    .select(
        "e.*",
        F.col("sub.id").alias("subscription_id"),
        F.col("sub.plan"),
        F.col("sub.monthly_fee"),
    )
)

scored = (
    attributed
    .select(
        "id",
        "created_at",
        "k",
        "subscription_id",
        "plan",
        F.sum("subscription_id").over(w_ordered).alias("running_subscription_id"),
        F.ntile(4).over(w2).alias("monthly_fee_bucket"),
        F.last("plan", ignorenulls=True).over(w3).alias("last_known_plan"),
    )
)

scored_carts = (
    scored
    .select(
        "*",
        F.dense_rank().over(w4).alias("monthly_fee_bucket_dense_rank"),
        F.round((F.col("subscription_id") / F.sum("subscription_id").over(w5)), 4).alias("subscription_id_share"),
        F.avg("subscription_id").over(w6).alias("subscription_id_avg_5"),
        F.avg("monthly_fee_bucket").over(w_hist).alias("plan_avg_monthly_fee_bucket"),
        F.lead("monthly_fee_bucket", 1).over(w8).alias("next_monthly_fee_bucket"),
    )
)

with_windows = (
    scored_carts
    .withColumn("running_min_next_monthly_fee_bucket", F.min("next_monthly_fee_bucket").over(w9))
    .withColumn("running_count_next_monthly_fee_bucket", F.count("next_monthly_fee_bucket").over(w9))
    .withColumn("running_sum_next_monthly_fee_bucket", F.sum("next_monthly_fee_bucket").over(w9))
    .withColumn("subscription_id_avg_5_count_2h", F.count("subscription_id_avg_5").over(w10))
)

prev_events = with_windows.withColumn("prev_created_at", F.lag("created_at").over(w11))

gaps = (
    prev_events
    .withColumn("is_new_session", F.when(F.col("prev_created_at").isNull() | (F.col("created_at") > F.col("prev_created_at") + F.expr("INTERVAL 30 MINUTES")), 1).otherwise(0))
)

islands = (
    gaps
    .select(
        "id",
        "created_at",
        "k",
        "subscription_id",
        "plan",
        "running_subscription_id",
        "monthly_fee_bucket",
        "last_known_plan",
        "subscription_id_share",
        "plan_avg_monthly_fee_bucket",
        "running_min_next_monthly_fee_bucket",
        "running_sum_next_monthly_fee_bucket",
        F.sum("is_new_session").over(w3).alias("session_no"),
    )
)

totals = (
    islands
    .groupBy(
        F.when(F.col("running_min_next_monthly_fee_bucket") >= 6.9, "HIGH").otherwise("LOW").alias("running_min_next_monthly_fee_bucket_tier"),
        F.when(F.col("subscription_id_share") >= 0.81, "LARGE").otherwise("SMALL").alias("subscription_id_share_tier"),
        "subscription_id",
    )
    .agg(
        F.round(F.sum("session_no") / F.nullif(F.count(F.when(F.col("k") == "weight_kg", 1)), F.lit(0)), 2).alias("session_no_per_weight_kg"),
        F.count_if(F.col("k") == "device").alias("device_events"),
        F.countDistinct("k").alias("distinct_ks"),
        F.count("*").alias("row_count"),
        F.round(F.avg("running_min_next_monthly_fee_bucket"), 2).alias("avg_running_min_next_monthly_fee_bucket"),
    )
)

result = (
    totals
    .select(
        "running_min_next_monthly_fee_bucket_tier",
        "subscription_id_share_tier",
        "subscription_id",
        "session_no_per_weight_kg",
        "distinct_ks",
    )
)
