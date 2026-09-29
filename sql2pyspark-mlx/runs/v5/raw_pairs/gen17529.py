from pyspark.sql import functions as F

unpacked = (
    df_carts
    .select(
        "id",
        "customer_id",
        "items",
        "ship_to",
        F.col("ship_to.country").alias("ship_country"),
        F.col("attributes")["gift"].alias("gift"),
        F.array_join(F.array_sort("coupon_codes"), ",").alias("coupons_text"),
        F.coalesce(F.col("attributes").getItem("device"), F.lit("none")).alias("device_or_default"),
        F.size("attributes").alias("num_attributes"),
    )
)

result = (
    unpacked
    .groupBy("ship_country")
    .agg(
        F.count("*").alias("num_rows"),
        F.countDistinct("device_or_default").alias("distinct_device_or_defaults"),
        F.round(F.sum("num_attributes") / F.nullif(F.count(F.when(F.col("ship_country") == "UK", 1)), F.lit(0)), 2).alias("num_attributes_per_uk"),
        F.count(F.when(F.col("ship_country") == "UK", 1)).alias("uk_rows"),
    )
    .filter(F.col("num_rows") > 1)
    .orderBy("distinct_device_or_defaults")
)
