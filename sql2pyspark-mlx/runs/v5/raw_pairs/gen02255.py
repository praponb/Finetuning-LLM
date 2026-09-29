from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status", "plan").orderBy(F.col("age").desc_nulls_last())
w2 = Window.partitionBy("customer_id", "city")
w3 = Window.partitionBy("customer_id").orderBy("customer_age", "id", "subscription_id")
w4 = Window.partitionBy("plan").orderBy("customer_age_bucket", "id", "subscription_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("customer_id", "city").orderBy("customer_age", "id", "subscription_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

joined = (
    df_subscriptions
    .alias("src")
    .join(df_customers.alias("p"), F.col("src.customer_id").eqNullSafe(F.col("p.id")), "inner")
    .filter(F.col("p.is_active"))
    .select("src.*", F.col("p.age"), F.col("p.name"))
)

deduped = (
    joined
    .withColumn("rank_in_group", F.rank().over(w))
    .filter(F.col("rank_in_group") == 1)
)

with_window = (
    deduped
    .alias("e")
    .join(df_subscriptions.alias("sub"), (F.col("e.customer_id") == F.col("sub.customer_id")) & F.col("e.start_date").between(F.col("sub.start_date"), F.col("sub.end_date")), "left")
    .select(
        "e.*",
        F.col("sub.id").alias("subscription_id"),
        F.col("sub.plan").alias("subscription_plan"),
        F.col("sub.monthly_fee").alias("subscription_monthly_fee"),
    )
)

with_dims = (
    with_window
    .alias("e")
    .join(df_customers.alias("p"), F.col("e.customer_id").eqNullSafe(F.col("p.id")), "left")
    .select(
        "e.*",
        F.col("p.age").alias("customer_age"),
        F.col("p.city"),
        F.col("p.email"),
    )
)

metrics = (
    with_dims
    .withColumn("subscription_id_share", F.round((F.col("subscription_id") / F.sum("subscription_id").over(w2)), 4))
    .withColumn("customer_age_bucket", F.ntile(4).over(w3))
)

metrics_subscriptions = (
    metrics
    .select(
        "id",
        "customer_id",
        "end_date",
        "status",
        "name",
        "rank_in_group",
        "subscription_id",
        "subscription_plan",
        "subscription_monthly_fee",
        "customer_age",
        "subscription_id_share",
        F.sum("customer_age_bucket").over(w4).alias("cumulative_customer_age_bucket"),
        F.last("subscription_plan", ignorenulls=True).over(w5).alias("last_known_subscription_plan"),
    )
)

daily = (
    metrics_subscriptions
    .groupBy(
        F.when(F.col("subscription_monthly_fee") >= 191, "LARGE").otherwise("SMALL").alias("subscription_monthly_fee_tier"),
    )
    .agg(
        F.sum(F.when(F.col("subscription_plan") == "enterprise", F.col("customer_age")).otherwise(0)).alias("enterprise_customer_age"),
        F.percentile_approx("subscription_monthly_fee", 0.95).alias("subscription_monthly_fee_p95"),
        F.percentile_approx("customer_age", 0.5).alias("customer_age_p50"),
    )
)

result = (
    daily
    .select(
        "subscription_monthly_fee_tier",
        "subscription_monthly_fee_p95",
        "customer_age_p50",
    )
)
