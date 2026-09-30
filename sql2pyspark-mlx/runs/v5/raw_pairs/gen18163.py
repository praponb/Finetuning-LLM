from pyspark.sql import functions as F

exploded = (
    df_carts
    .withColumn("li_item", F.explode_outer("items"))
    .select(
        "id",
        "customer_id",
        F.col("li_item.product_id"),
        F.col("li_item.qty"),
        F.col("li_item.promo"),
    )
)

result = (
    exploded
    .groupBy(
        F.when(F.col("qty") >= 5, "PREMIUM").otherwise("STANDARD").alias("qty_tier"),
    )
    .agg(
        F.size(F.collect_set("promo")).alias("num_promos"),
        F.count("*").alias("cnt"),
        F.size(F.collect_set("promo")).alias("num_promos_2"),
        F.round(F.avg("qty"), 2).alias("avg_qty"),
        F.max(F.when(F.col("promo") == "SPRING10", F.col("product_id"))).alias("spring10_max_product_id"),
        F.count_if(F.col("promo") == "SPRING10").alias("spring10_events"),
    )
    .orderBy(F.col("avg_qty").desc())
)
