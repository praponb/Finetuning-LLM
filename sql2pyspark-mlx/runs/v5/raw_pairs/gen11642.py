from pyspark.sql import functions as F

lines = (
    df_carts
    .withColumn("elem", F.explode("items"))
    .select(
        "id",
        "customer_id",
        "created_at",
        "coupon_codes",
        F.col("elem.product_id"),
        F.col("elem.qty"),
        F.col("elem.price"),
        F.col("elem.promo"),
    )
)

result = (
    lines
    .withColumn("hour_window", F.date_trunc("HOUR", "created_at"))
    .withColumn("price_tier", F.when(F.col("price") >= 211, "HIGH").otherwise("LOW"))
    .withColumn("qty_tier", F.when(F.col("qty") >= 5, "PREMIUM").otherwise("STANDARD"))
    .groupingSets(
        [["hour_window", "price_tier", "qty_tier"], ["hour_window", "price_tier"], ["hour_window"]],
        "hour_window",
        "price_tier",
        "qty_tier",
    )
    .agg(
        F.sum("product_id").alias("total_product_id"),
        F.count(F.when(F.col("promo") == "SPRING10", 1)).alias("spring10_rows"),
    )
)
