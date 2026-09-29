from pyspark.sql import functions as F

flattened = (
    df_carts
    .select(
        "id",
        "customer_id",
        "created_at",
        "coupon_codes",
        F.explode_outer("attributes").alias("k", "v"),
    )
)

lines = (
    flattened
    .select("id", "customer_id", "k", F.explode("coupon_codes").alias("element"))
)

joined = (
    lines
    .alias("a")
    .join(df_customers.alias("p"), F.col("a.customer_id") == F.col("p.id"), "left")
    .select("a.*", F.col("p.name"), F.col("p.email"))
)

enriched = (
    joined
    .alias("src")
    .join(df_customers.alias("c"), F.col("src.customer_id") == F.col("c.id"), "left")
    .select(
        "src.*",
        F.col("c.email").alias("customer_email"),
        F.col("c.name").alias("customer_name"),
        F.col("c.city"),
    )
)

joined_carts = (
    enriched
    .alias("e")
    .join(df_customers.alias("c"), F.col("e.customer_id") == F.col("c.id"), "inner")
    .filter(F.col("c.name").like("I%"))
    .select(
        "e.customer_id",
        "e.k",
        "e.element",
        "e.name",
        "e.email",
        "e.customer_email",
        "e.city",
        F.col("c.segment"),
    )
)

filtered = (
    joined_carts
    .filter(F.col("k").isNotNull() & F.lower("name").like("%enr%"))
    .withColumn("customer_email_normalized", F.upper(F.trim("customer_email")))
    .withColumn("city_filled", F.coalesce("city", F.lit("NONE")))
)

result = (
    filtered
    .groupingSets([["segment", "element"], ["segment"]], "segment", "element")
    .agg(
        F.count("*").alias("event_count"),
        F.count(F.when(F.col("k") == "device", 1)).alias("device_rows"),
        F.count(F.when(F.col("element") == "VIP", 1)).alias("vip_count"),
        F.count_if(F.col("city") == "Berlin").alias("berlin_events"),
        F.grouping("segment").alias("grouping_level"),
    )
    .orderBy(F.col("berlin_events").desc())
)
