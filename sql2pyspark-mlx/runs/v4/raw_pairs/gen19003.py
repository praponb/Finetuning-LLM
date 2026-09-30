from pyspark.sql import functions as F

flattened = (
    df_carts
    .select("id", "customer_id", "created_at", "coupon_codes", F.inline("items"))
)

joined = (
    flattened
    .alias("src")
    .join(df_customers.alias("c"), F.col("src.customer_id") == F.col("c.id"), "left")
    .select("src.*", F.col("c.age"), F.col("c.city"))
)

filtered = (
    joined
    .filter((F.col("created_at") > F.to_timestamp(F.lit("2025-03-02 13:00:00"))) & F.col("city").isNotNull())
    .withColumn("created_at_weekofyear", F.weekofyear("created_at"))
)

base = (
    filtered
    .filter((F.col("created_at") > F.to_date(F.lit("2025-03-01"))) & (F.col("promo") == "WELCOME5"))
    .select(
        "id",
        "customer_id",
        "coupon_codes",
        "product_id",
        "qty",
        "age",
        "city",
        "created_at_weekofyear",
        F.round("created_at_weekofyear", 1).alias("created_at_weekofyear_rounded"),
    )
)

result = (
    base
    .groupBy(F.when(F.col("qty") >= 5, "HIGH").otherwise("LOW").alias("qty_tier"))
    .agg(
        F.count("*").alias("cnt"),
        F.count("*").alias("cnt_2"),
        F.avg(F.when(F.col("city") == "Tokyo", F.col("product_id"))).alias("tokyo_avg_product_id"),
        F.round(F.avg("age"), 2).alias("avg_age"),
        F.countDistinct("city").alias("distinct_cities"),
    )
    .filter(F.col("distinct_cities") > 1)
)
