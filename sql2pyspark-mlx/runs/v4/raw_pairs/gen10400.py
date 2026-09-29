from pyspark.sql import functions as F

monthly_fee_by_customer_id = df_subscriptions.groupBy("customer_id").agg(F.sum("monthly_fee").alias("sum_subscription_monthly_fee"))

exploded = (
    df_carts
    .select(
        "id",
        "customer_id",
        "created_at",
        F.explode_outer("coupon_codes").alias("code"),
    )
)

with_dims = (
    exploded
    .alias("a")
    .join(df_customers.alias("d"), F.col("a.customer_id") == F.col("d.id"), "inner")
    .select("a.*", F.col("d.city"), F.col("d.country"))
)

with_dims_carts = (
    with_dims
    .alias("s")
    .join(df_customers.alias("b"), F.col("s.customer_id") == F.col("b.id"), "inner")
    .select("s.*", F.col("b.name"))
)

with_dims_carts_2 = (
    with_dims_carts
    .alias("a")
    .join(df_customers.alias("d"), F.col("a.customer_id") == F.col("d.id"), "left")
    .select(
        "a.*",
        F.col("d.city").alias("customer_city"),
        F.col("d.age"),
        F.col("d.signup_date"),
    )
)

enriched = (
    with_dims_carts_2
    .alias("s")
    .join(df_customers.alias("d"), F.col("s.customer_id") == F.col("d.id"), "left")
    .select(
        "s.*",
        F.col("d.signup_date").alias("customer_signup_date"),
        F.col("d.email"),
    )
)

joined = (
    enriched
    .alias("e")
    .join(df_customers.alias("d"), F.col("e.customer_id") == F.col("d.id"), "left")
    .select("e.*", F.col("d.name").alias("customer_name"))
)

with_dims_carts_3 = (
    joined
    .alias("s")
    .join(df_customers.alias("b"), F.col("s.customer_id") == F.col("b.id"), "left")
    .select(
        "s.*",
        F.col("b.city").alias("customer_city_2"),
        F.col("b.signup_date").alias("customer_signup_date_2"),
    )
)

result = (
    with_dims_carts_3
    .join(monthly_fee_by_customer_id, on="customer_id", how="left")
    .select(
        "customer_id",
        "created_at",
        "code",
        "city",
        "country",
        "name",
        "customer_city",
        "signup_date",
        "customer_signup_date",
        F.col("sum_subscription_monthly_fee"),
    )
    .orderBy(F.col("customer_city").desc())
)
