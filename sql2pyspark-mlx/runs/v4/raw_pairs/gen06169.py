from pyspark.sql import functions as F

parsed = (
    df_carts
    .filter(F.col("customer_id").isNull())
    .select(
        "id",
        "customer_id",
        "items",
        "ship_to",
        F.aggregate("items", F.lit(0.0), lambda acc, item: acc + item["qty"] * item["price"], lambda acc: F.round(acc, 2)).alias("items_total"),
        F.col("ship_to.city").alias("ship_city"),
        F.array_join(F.array_sort("coupon_codes"), ",").alias("coupons_text"),
        F.array_contains(F.map_keys("attributes"), "weight_kg").alias("has_weight_kg"),
        F.exists("items", lambda item: item["price"] > 100).alias("has_premium_item"),
    )
)

result = (
    parsed
    .rollup(
        "ship_city",
        F.when(F.col("items_total") >= 1410, "LARGE").otherwise("SMALL").alias("items_total_tier"),
    )
    .agg(
        F.countDistinct("ship_city").alias("distinct_ship_cities"),
        F.countDistinct("customer_id").alias("distinct_customer_ids"),
        F.count(F.when(F.col("ship_city") == "Tokyo", 1)).alias("tokyo_count"),
        F.avg("items_total").alias("avg_items_total"),
        F.percentile_approx("items_total", [0.25, 0.5, 0.75]).alias("items_total_quartiles"),
    )
    .orderBy(F.col("distinct_ship_cities").asc_nulls_first())
)
