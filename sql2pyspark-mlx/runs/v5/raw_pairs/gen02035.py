from pyspark.sql import functions as F

exploded = (
    df_carts
    .withColumn("item", F.explode_outer("items"))
    .select(
        "id",
        "created_at",
        "coupon_codes",
        F.col("item.product_id"),
        F.col("item.qty"),
        F.col("item.price"),
    )
)

result = (
    exploded
    .groupBy(
        F.when(F.col("qty") >= 5, "PREMIUM").otherwise("STANDARD").alias("qty_tier"),
    )
    .agg(
        F.min("created_at").alias("first_created_at"),
        F.percentile_approx("qty", [0.25, 0.5, 0.75]).alias("qty_quartiles"),
        F.count("*").alias("cnt"),
        F.sum("product_id").alias("total_product_id"),
        F.percentile_approx("product_id", 0.5).alias("product_id_p50"),
        F.max("created_at").alias("last_seen"),
    )
)
