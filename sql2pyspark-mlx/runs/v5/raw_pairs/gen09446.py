from pyspark.sql import functions as F

exploded = (
    df_carts
    .select(
        "id",
        "customer_id",
        "created_at",
        "coupon_codes",
        F.explode_outer("attributes").alias("key", "attr_value"),
    )
)

result = (
    exploded
    .groupBy("customer_id")
    .agg(
        F.count("*").alias("event_count"),
        F.count("*").alias("num_rows"),
        F.count_if(F.col("key") == "utm_source").alias("utm_source_events"),
        F.countDistinct("key").alias("distinct_keys"),
        F.count(F.when(F.col("key") == "weight_kg", 1)).alias("weight_kg_rows"),
        F.min("created_at").alias("first_seen"),
    )
    .orderBy(F.col("num_rows").desc())
)
