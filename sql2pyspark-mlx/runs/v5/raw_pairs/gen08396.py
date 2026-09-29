from pyspark.sql import functions as F

flattened = (
    df_carts
    .withColumn("item", F.explode_outer("items"))
    .select(
        "id",
        "customer_id",
        "coupon_codes",
        F.col("item.product_id"),
        F.col("item.qty"),
        F.col("item.price"),
    )
)

result = (
    flattened
    .groupBy(
        F.when(F.col("product_id") >= 10, "LARGE").otherwise("SMALL").alias("product_id_tier"),
    )
    .agg(
        F.count("*").alias("event_count"),
        F.percentile_approx("product_id", 0.95).alias("product_id_p95"),
        F.sum("price").alias("total_price"),
        F.max("qty").alias("max_qty"),
    )
    .filter(F.col("max_qty") > 0)
)
