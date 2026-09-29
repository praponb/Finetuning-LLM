from pyspark.sql import functions as F

flattened = (
    df_carts
    .withColumn("elem", F.explode("items"))
    .select(
        "id",
        "customer_id",
        "coupon_codes",
        F.col("elem.product_id"),
        F.col("elem.qty"),
        F.col("elem.price"),
    )
)

result = (
    flattened
    .cube(
        "customer_id",
        F.when(F.col("price") >= 256, "PREMIUM").otherwise("STANDARD").alias("price_tier"),
    )
    .agg(
        F.countDistinct("customer_id").alias("distinct_customer_ids"),
        F.min("price").alias("min_price"),
        F.percentile_approx("price", [0.25, 0.5, 0.75]).alias("price_quartiles"),
    )
    .filter(F.col("distinct_customer_ids") > 1)
)
