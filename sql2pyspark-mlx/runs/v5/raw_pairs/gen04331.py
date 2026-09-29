from pyspark.sql import functions as F

monthly_fee_by_customer_id = df_subscriptions.groupBy("customer_id").agg(F.min("monthly_fee").alias("min_subscription_monthly_fee"))

exploded = (
    df_carts
    .select(
        "id",
        "customer_id",
        "created_at",
        "coupon_codes",
        F.explode("attributes").alias("key", "attr_value"),
    )
)

exploded_carts = (
    exploded
    .select(
        "id",
        "customer_id",
        "created_at",
        "key",
        "attr_value",
        F.explode("coupon_codes").alias("element"),
    )
)

with_dims = (
    exploded_carts
    .alias("a")
    .join(df_customers.alias("p"), F.col("a.customer_id") == F.col("p.id"), "left")
    .select("a.*", F.col("p.city"))
)

with_dims_carts = (
    with_dims
    .alias("src")
    .join(df_customers.alias("p"), F.col("src.customer_id") == F.col("p.id"), "left")
    .filter(~F.col("p.is_active"))
    .select(
        "src.customer_id",
        "src.created_at",
        "src.key",
        "src.attr_value",
        "src.element",
        "src.city",
        F.col("p.segment"),
        F.col("p.age"),
        F.col("p.city").alias("customer_city"),
    )
)

result = (
    with_dims_carts
    .join(monthly_fee_by_customer_id, on="customer_id", how="left")
    .select(
        "customer_id",
        "created_at",
        "key",
        "attr_value",
        "element",
        "segment",
        "age",
        "customer_city",
        F.col("min_subscription_monthly_fee"),
    )
)
