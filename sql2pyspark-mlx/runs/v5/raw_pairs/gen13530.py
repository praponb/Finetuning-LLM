from pyspark.sql import functions as F

unpacked = (
    df_carts
    .filter(F.col("customer_id").isin([5, 17, 19]))
    .select(
        "id",
        "customer_id",
        "created_at",
        "coupon_codes",
        "items",
        "ship_to",
        "attributes",
        F.col("ship_to.city").alias("ship_city"),
        F.coalesce(F.col("ship_to.zip"), F.lit("UNKNOWN")).alias("zip_or_unknown"),
        F.coalesce(F.col("attributes").getItem("utm_source"), F.lit("none")).alias("utm_source_or_default"),
        F.array_join(F.array_sort("coupon_codes"), ",").alias("coupons_text"),
    )
)

result = (
    unpacked
    .rollup("ship_city", "customer_id")
    .agg(
        F.count(F.when(F.col("ship_city") == "London", 1)).alias("london_count"),
        F.count(F.when(F.col("ship_city") == "Singapore", 1)).alias("singapore_count"),
        F.countDistinct("utm_source_or_default").alias("distinct_utm_source_or_defaults"),
        F.grouping("ship_city").alias("grouping_level"),
    )
    .orderBy(F.col("ship_city").desc_nulls_last())
)
