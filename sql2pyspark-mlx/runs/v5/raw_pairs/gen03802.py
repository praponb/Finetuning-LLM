from pyspark.sql import functions as F

flattened = (
    df_carts
    .select("id", "customer_id", "created_at", F.explode("coupon_codes").alias("tag"))
)

enriched = (
    flattened
    .alias("e")
    .join(df_customers.alias("c"), F.col("e.customer_id") == F.col("c.id"), "left")
    .filter(F.col("c.age") <= 37)
    .select(
        "e.customer_id",
        "e.created_at",
        "e.tag",
        F.col("c.city"),
        F.col("c.age"),
        F.col("c.name"),
    )
)

filtered = (
    enriched
    .filter(F.col("name").isNotNull() & (F.col("created_at") >= F.lit("2025-03-01 22:00:00").cast("timestamp")))
    .select("*", F.coalesce("city", F.lit("Unknown")).alias("city_filled"))
)

joined = (
    filtered
    .alias("src")
    .join(df_customers.alias("p"), F.col("src.customer_id") == F.col("p.id"), "left")
    .select(
        "src.customer_id",
        "src.created_at",
        "src.age",
        F.col("p.city").alias("customer_city"),
    )
)

joined_carts = (
    joined
    .alias("e")
    .join(df_customers.alias("b"), F.col("e.customer_id") == F.col("b.id"), "left")
    .select("e.*", F.col("b.city"))
)

summary = (
    joined_carts
    .groupBy(F.when(F.col("age") >= 61, "HIGH").otherwise("LOW").alias("age_tier"))
    .agg(
        F.countDistinct("customer_city").alias("distinct_customer_cities"),
        F.count(F.when(F.col("customer_city") == "Tokyo", 1)).alias("tokyo_rows"),
        F.percentile_approx("age", 0.95).alias("age_p95"),
        ((F.unix_timestamp(F.max("created_at")) - F.unix_timestamp(F.min("created_at"))) / 60).alias("duration_min"),
        F.countDistinct("customer_city").alias("distinct_customer_cities_2"),
    )
)

result = (
    summary
    .select(
        "age_tier",
        "distinct_customer_cities",
        "tokyo_rows",
        "age_p95",
        "duration_min",
        "distinct_customer_cities_2",
    )
)
