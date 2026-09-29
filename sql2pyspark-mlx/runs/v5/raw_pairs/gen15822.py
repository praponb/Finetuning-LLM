from pyspark.sql import functions as F

monthly_fee_by_customer_id = df_subscriptions.groupBy("customer_id").agg(F.sum("monthly_fee").alias("sum_subscription_monthly_fee"))

lines = (
    df_carts
    .select(
        "id",
        "customer_id",
        "coupon_codes",
        F.explode_outer("attributes").alias("key", "v"),
    )
)

valid = (
    lines
    .filter((~F.col("key").eqNullSafe(F.lit("utm_source"))) & F.col("customer_id").isin([8, 15, 25, 27]))
    .select(
        "id",
        "customer_id",
        "coupon_codes",
        "key",
        F.upper("key").alias("key_upper"),
        F.coalesce("key", F.lit("NONE")).alias("key_filled"),
    )
)

with_dims = (
    valid
    .alias("src")
    .join(df_customers.alias("d"), F.col("src.customer_id").eqNullSafe(F.col("d.id")), "left")
    .select("src.*", F.col("d.country"))
)

enriched = (
    with_dims
    .alias("e")
    .join(df_customers.alias("c"), F.col("e.customer_id") == F.col("c.id"), "left")
    .select("e.*", F.col("c.credit_limit"), F.col("c.email"))
)

enriched_carts = (
    enriched
    .alias("a")
    .join(df_customers.alias("p"), F.col("a.customer_id") == F.col("p.id"), "left")
    .select(
        "a.customer_id",
        "a.key_upper",
        "a.key_filled",
        "a.credit_limit",
        "a.email",
        F.col("p.credit_limit").alias("customer_credit_limit"),
        F.col("p.country").alias("customer_country"),
    )
)

result = (
    enriched_carts
    .join(monthly_fee_by_customer_id, on="customer_id", how="left")
    .select(
        "customer_id",
        "key_upper",
        "credit_limit",
        "email",
        "customer_credit_limit",
        "customer_country",
        F.when(F.col("customer_country").rlike("^(DE|UK)$"), "PRIORITY").when(F.col("customer_country").rlike("^(TH|US)$"), "GROUP_B").otherwise("CORE").alias("customer_country_zone"),
        F.col("sum_subscription_monthly_fee"),
    )
)
