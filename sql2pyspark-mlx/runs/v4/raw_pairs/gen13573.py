from pyspark.sql import functions as F

unpacked = (
    df_carts
    .filter(F.array_contains("coupon_codes", "VIP"))
    .select(
        "id",
        "created_at",
        "coupon_codes",
        "attributes",
        F.col("ship_to.city").alias("ship_city"),
        F.size(F.filter("items", lambda item: item["qty"] >= 3)).alias("bulk_lines"),
        F.array_contains("coupon_codes", "BUNDLE").alias("used_bundle"),
        F.array_contains(F.map_keys("attributes"), "utm_source").alias("has_utm_source"),
        F.exists("items", lambda item: item["price"] > 100).alias("has_premium_item"),
    )
)

result = (
    unpacked
    .groupBy("ship_city")
    .agg(
        F.size(F.collect_set("ship_city")).alias("num_ship_cities"),
        F.count_if(F.col("ship_city") == "Singapore").alias("singapore_events"),
        F.max(F.when(F.col("ship_city") == "London", F.col("bulk_lines"))).alias("london_max_bulk_lines"),
        F.count(F.when(F.col("ship_city") == "Berlin", 1)).alias("berlin_count"),
        F.sum(F.when(F.col("ship_city") == "Singapore", F.col("bulk_lines")).otherwise(0)).alias("singapore_bulk_lines"),
    )
)
