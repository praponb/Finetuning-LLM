from pyspark.sql import functions as F

exploded = (
    df_carts
    .select("*", F.posexplode_outer("items").alias("pos", "li_item"))
    .select(
        "id",
        "created_at",
        "pos",
        F.col("li_item.product_id"),
        F.col("li_item.qty"),
        F.col("li_item.price"),
        F.col("li_item.promo"),
    )
)

result = (
    exploded
    .groupBy(
        F.when(F.col("product_id") >= 8, "LARGE").otherwise("SMALL").alias("product_id_tier"),
        F.date_trunc("DAY", "created_at").alias("activity_day"),
    )
    .agg(
        F.count(F.when(F.col("promo") == "WELCOME5", 1)).alias("welcome5_count"),
        F.avg("product_id").alias("avg_product_id"),
        F.percentile_approx("price", 0.5).alias("price_p50"),
        F.min("created_at").alias("started_at"),
        F.min("created_at").alias("first_seen"),
        F.count_if(F.col("promo") == "SPRING10").alias("spring10_events"),
    )
    .orderBy(F.col("product_id_tier").desc_nulls_first())
)
