from pyspark.sql import functions as F

exploded = (
    df_carts
    .withColumn("li_item", F.explode_outer("items"))
    .select(
        "id",
        "customer_id",
        "created_at",
        "coupon_codes",
        F.col("li_item.product_id"),
        F.col("li_item.qty"),
        F.col("li_item.price"),
        F.col("li_item.promo"),
    )
)

result = (
    exploded
    .groupBy(
        F.date_trunc("HOUR", "created_at").alias("hour_window"),
        F.when(F.col("qty") >= 5, "HIGH").otherwise("LOW").alias("qty_tier"),
        F.date_trunc("DAY", "created_at").alias("activity_day"),
    )
    .agg(
        F.count("*").alias("row_count"),
        F.max("created_at").alias("last_seen"),
        F.count(F.when(F.col("promo") == "BUNDLE", 1)).alias("bundle_rows"),
        F.percentile_approx("product_id", [0.25, 0.5, 0.75]).alias("product_id_quartiles"),
        F.countDistinct("promo").alias("distinct_promos"),
    )
)
