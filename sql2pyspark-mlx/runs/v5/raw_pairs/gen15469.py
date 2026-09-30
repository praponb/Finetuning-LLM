from pyspark.sql import functions as F

unpacked = (
    df_carts
    .select(
        "id",
        "customer_id",
        "items",
        "attributes",
        F.size("attributes").alias("num_attributes"),
        F.exists("coupon_codes", lambda c: c.like("W%")).alias("has_welcome_code"),
        F.col("attributes")["gift"].alias("gift"),
        F.exists("items", lambda i: i["price"] > 100).alias("has_premium_item"),
    )
)

result = (
    unpacked
    .groupBy(
        F.when(F.col("num_attributes") >= 79, "LARGE").otherwise("SMALL").alias("num_attributes_tier"),
    )
    .agg(
        F.count("*").alias("row_count"),
        F.round(F.avg("num_attributes"), 2).alias("avg_num_attributes"),
        F.count("*").alias("row_count_2"),
    )
)
