from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status", "subscription_plan").orderBy(F.col("monthly_fee").desc_nulls_first(), "id", "subscription_id")
w2 = Window.partitionBy("plan").orderBy("end_date", "id", "subscription_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_recent = Window.partitionBy("customer_id").orderBy("start_date", "id", "subscription_id").rowsBetween(-3, Window.currentRow)
w4 = Window.partitionBy("plan").orderBy(F.col("cumulative_subscription_id").asc_nulls_last(), "id", "subscription_id")
w5 = Window.partitionBy("customer_id", "plan").orderBy("subscription_id_avg_3", "id", "subscription_id").rowsBetween(-3, Window.currentRow)
w6 = Window.partitionBy("customer_id").orderBy("start_date", "id", "subscription_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

matched = (
    df_subscriptions
    .alias("s")
    .join(df_subscriptions.alias("sub"), (F.col("s.customer_id") == F.col("sub.customer_id")) & (F.col("s.start_date") >= F.col("sub.start_date")) & (F.col("s.start_date") < F.col("sub.end_date") + F.expr("INTERVAL 7 DAYS")), "left")
    .select(
        "s.*",
        F.col("sub.id").alias("subscription_id"),
        F.col("sub.plan").alias("subscription_plan"),
        F.col("sub.monthly_fee").alias("subscription_monthly_fee"),
    )
)

ranked = (
    matched
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
    .select("id", "customer_id", "plan", "start_date", "end_date", "subscription_id")
)

scored = (
    ranked
    .select(
        "*",
        F.sum("subscription_id").over(w2).alias("cumulative_subscription_id"),
        F.avg("subscription_id").over(w_recent).alias("subscription_id_avg_3"),
    )
)

deduped = (
    scored
    .withColumn("row_num", F.row_number().over(w4))
    .filter(F.col("row_num") == 1)
)

base = (
    deduped
    .filter(F.col("subscription_id") <= 5)
    .withColumn("row_num_minus_subscription_id", (F.col("row_num") - F.col("subscription_id")))
)

scored_subscriptions = (
    base
    .select(
        "*",
        F.min("cumulative_subscription_id").over(w5).alias("last4_min_cumulative_subscription_id"),
        F.avg("cumulative_subscription_id").over(w5).alias("last4_avg_cumulative_subscription_id"),
        F.sum("cumulative_subscription_id").over(w5).alias("last4_sum_cumulative_subscription_id"),
        F.sum("row_num_minus_subscription_id").over(w6).alias("running_row_num_minus_subscription_id"),
    )
)

base_subscriptions = (
    scored_subscriptions
    .filter(F.col("row_num") >= 1)
    .select(
        "*",
        (F.col("last4_avg_cumulative_subscription_id") + F.col("running_row_num_minus_subscription_id")).alias("last4_avg_cumulative_subscription_id_plus_running_row_num_minus_subscription_id"),
    )
)

stats = (
    base_subscriptions
    .groupBy(
        F.when(F.col("running_row_num_minus_subscription_id") >= 309, "HIGH").otherwise("LOW").alias("running_row_num_minus_subscription_id_tier"),
    )
    .agg(
        F.round(F.avg("subscription_id"), 2).alias("avg_subscription_id"),
        F.percentile_approx("cumulative_subscription_id", 0.95).alias("cumulative_subscription_id_p95"),
        F.count(F.when(F.col("plan") == "enterprise", 1)).alias("enterprise_count"),
        F.percentile_approx("cumulative_subscription_id", [0.25, 0.5, 0.75]).alias("cumulative_subscription_id_quartiles"),
        F.round(F.sum("last4_min_cumulative_subscription_id") / F.nullif(F.count(F.when(F.col("plan") == "basic", 1)), F.lit(0)), 2).alias("last4_min_cumulative_subscription_id_per_basic"),
    )
)

result = (
    stats
    .select(
        "running_row_num_minus_subscription_id_tier",
        "avg_subscription_id",
        "cumulative_subscription_id_p95",
        "last4_min_cumulative_subscription_id_per_basic",
    )
    .orderBy(F.col("avg_subscription_id").desc_nulls_last())
)
