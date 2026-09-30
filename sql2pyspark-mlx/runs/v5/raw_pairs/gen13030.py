from pyspark.sql import functions as F

parsed = (
    df_carts
    .filter(F.col("customer_id").isin([16, 18, 19]))
    .select(
        "id",
        "customer_id",
        "items",
        "ship_to",
        "attributes",
        F.array_contains(F.map_keys("attributes"), "weight_kg").alias("has_weight_kg"),
        F.aggregate("items", F.lit(0.0), lambda acc, item: acc + item["qty"] * item["price"], lambda acc: F.round(acc, 2)).alias("cart_value"),
        F.array_join(F.array_sort("coupon_codes"), ",").alias("coupons_text"),
        F.coalesce(F.col("attributes")["gift"], F.lit("direct")).alias("gift_or_default"),
    )
)

result = (
    parsed
    .rollup("customer_id")
    .agg(
        F.count("*").alias("event_count"),
        F.avg("cart_value").alias("avg_cart_value"),
        F.percentile_approx("cart_value", 0.95).alias("cart_value_p95"),
        F.countDistinct("id").alias("distinct_ids"),
        F.max("cart_value").alias("max_cart_value"),
        F.grouping_id().alias("gid"),
    )
    .orderBy(F.col("max_cart_value").desc())
)
