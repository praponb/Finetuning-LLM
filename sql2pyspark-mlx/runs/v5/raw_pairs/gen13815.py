from pyspark.sql import functions as F

exploded = (
    df_carts
    .withColumn("line", F.explode("items"))
    .select(
        "id",
        "customer_id",
        "created_at",
        "coupon_codes",
        F.col("line.product_id"),
        F.col("line.qty"),
        F.col("line.price"),
    )
)

flattened = (
    exploded
    .select(
        "customer_id",
        "created_at",
        "price",
        F.explode("coupon_codes").alias("value"),
    )
)

enriched = (
    flattened
    .alias("e")
    .join(df_customers.alias("b"), F.col("e.customer_id") == F.col("b.id"), "left")
    .select("e.*", F.col("b.country"), F.col("b.email"))
)

base = (
    enriched
    .filter(F.col("email").isNotNull())
    .withColumn("country_lower", F.lower("country"))
    .withColumn("resolved_value", F.coalesce("value", F.lit("DIRECT")))
)

enriched_carts = (
    base
    .alias("a")
    .join(df_customers.alias("p"), F.col("a.customer_id") == F.col("p.id"), "inner")
    .select(
        "a.customer_id",
        "a.created_at",
        "a.price",
        "a.email",
        "a.resolved_value",
        F.col("p.email").alias("customer_email"),
        F.col("p.signup_date"),
    )
)

with_dims = (
    enriched_carts
    .alias("a")
    .join(df_customers.alias("c"), F.col("a.customer_id").eqNullSafe(F.col("c.id")), "left")
    .select(
        "a.*",
        F.col("c.email").alias("customer_email_2"),
        F.col("c.segment"),
        F.col("c.country"),
    )
)

summary = (
    with_dims
    .withColumn("activity_day", F.date_trunc("DAY", "created_at"))
    .withColumn("price_tier", F.when(F.col("price") >= 222, "PREMIUM").otherwise("STANDARD"))
    .groupingSets(
        [["activity_day", "price_tier"], ["activity_day"]],
        "activity_day",
        "price_tier",
    )
    .agg(
        F.sum(F.when(F.col("country") == "JP", F.col("price")).otherwise(0)).alias("jp_price"),
        F.count(F.when(F.col("segment") == "Consumer", 1)).alias("consumer_count"),
    )
)

result = (
    summary
    .select("activity_day", "jp_price")
    .orderBy(F.col("activity_day").desc())
)
