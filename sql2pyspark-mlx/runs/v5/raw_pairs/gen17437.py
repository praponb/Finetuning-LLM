from pyspark.sql import functions as F

unpacked = (
    df_carts
    .select(
        "id",
        "coupon_codes",
        "ship_to",
        F.exists("items", lambda x: x["price"] > 100).alias("has_premium_item"),
        F.coalesce(F.col("ship_to.zip"), F.lit("UNKNOWN")).alias("zip_or_unknown"),
        F.array_join(F.array_sort("coupon_codes"), ",").alias("coupons_text"),
        F.col("ship_to.city").alias("ship_city"),
    )
)

result = (
    unpacked
    .groupBy("ship_city")
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.count(F.when(F.col("ship_city") == "Singapore", 1)).alias("singapore_count"),
        F.sort_array(F.collect_set("ship_city")).alias("ship_city_list"),
        F.count(F.when(F.col("ship_city") == "Tokyo", 1)).alias("tokyo_rows"),
        F.countDistinct("id").alias("distinct_ids_2"),
    )
    .filter(F.col("distinct_ids") > 1)
    .orderBy(F.col("distinct_ids_2").desc())
)
