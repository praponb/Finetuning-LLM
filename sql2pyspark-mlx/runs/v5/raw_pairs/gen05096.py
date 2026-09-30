from pyspark.sql import functions as F

monthly_fee_by_customer_id = df_subscriptions.groupBy("customer_id").agg(F.avg("monthly_fee").alias("avg_subscription_monthly_fee"))

lines = (
    df_carts
    .withColumn("line", F.explode("items"))
    .select("id", "customer_id", "created_at", "coupon_codes", F.col("line.promo"))
)

lines_carts = (
    lines
    .select(
        "id",
        "customer_id",
        "created_at",
        "promo",
        F.explode("coupon_codes").alias("tag"),
    )
)

valid = (
    lines_carts
    .filter(F.col("created_at") >= F.to_date(F.lit("2025-03-01")))
    .select(
        "customer_id",
        "created_at",
        "tag",
        F.when(F.col("tag") == "BUNDLE", 1).otherwise(0).alias("is_bundle"),
        F.coalesce("promo", F.lit("UNKNOWN")).alias("resolved_promo"),
    )
)

joined = (
    valid
    .alias("a")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "left")
    .select(
        "a.customer_id",
        "a.is_bundle",
        F.col("c.credit_limit"),
        F.col("c.segment"),
        F.col("c.email"),
    )
)

joined_carts = (
    joined
    .alias("s")
    .join(df_customers.alias("p"), F.col("s.customer_id") == F.col("p.id"), "left")
    .select("s.*", F.col("p.email").alias("customer_email"))
)

result = (
    joined_carts
    .join(monthly_fee_by_customer_id, on="customer_id", how="left")
    .select(
        "customer_id",
        "is_bundle",
        "credit_limit",
        "email",
        F.col("avg_subscription_monthly_fee"),
    )
    .orderBy(F.col("credit_limit").desc())
)
