from pyspark.sql import functions as F

monthly_fee_by_customer_id = df_subscriptions.groupBy("customer_id").agg(F.max("monthly_fee").alias("max_subscription_monthly_fee"))

lines = (
    df_carts
    .select(
        "id",
        "customer_id",
        "created_at",
        "coupon_codes",
        F.explode_outer("attributes").alias("key", "v"),
    )
)

enriched = (
    lines
    .alias("e")
    .join(df_customers.alias("b"), F.col("e.customer_id") == F.col("b.id"), "inner")
    .select(
        "e.id",
        "e.customer_id",
        "e.created_at",
        "e.coupon_codes",
        "e.v",
        F.col("b.age"),
    )
)

valid = (
    enriched
    .filter((F.col("created_at") >= F.lit("2025-03-01 18:00:00").cast("timestamp")) & F.col("customer_id").isNotNull())
    .select(
        "customer_id",
        "v",
        F.round("age", 2).alias("age_rounded"),
        F.date_trunc("month", "created_at").alias("created_at_period"),
    )
)

cleaned = (
    valid
    .filter(F.col("customer_id").isNotNull())
    .select(
        "*",
        F.datediff(F.current_date(), "created_at_period").alias("days_since_created_at_period"),
    )
)

result = (
    cleaned
    .join(monthly_fee_by_customer_id, on="customer_id", how="left")
    .select(
        "customer_id",
        "v",
        "age_rounded",
        "days_since_created_at_period",
        F.col("max_subscription_monthly_fee"),
    )
    .orderBy("customer_id")
)
