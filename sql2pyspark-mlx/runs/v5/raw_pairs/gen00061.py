from pyspark.sql import functions as F

unpacked = (
    df_carts
    .select(
        "id",
        "created_at",
        "items",
        F.size("attributes").alias("num_attributes"),
        F.array_contains(F.map_keys("attributes"), "weight_kg").alias("has_weight_kg"),
        F.exists("items", lambda i: i["price"] > 100).alias("has_premium_item"),
        F.col("attributes").getItem("utm_source").alias("utm_source"),
        F.forall("items", lambda i: i["qty"] <= 5).alias("all_small_qty"),
    )
)

result = (
    unpacked
    .groupBy(
        F.when(F.col("num_attributes") >= 74, "PREMIUM").otherwise("STANDARD").alias("num_attributes_tier"),
    )
    .agg(
        F.countDistinct("utm_source").alias("distinct_utm_sources"),
        F.round(F.avg("num_attributes"), 2).alias("avg_num_attributes"),
    )
)
