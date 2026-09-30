from pyspark.sql import functions as F

flattened = (
    df_carts
    .select("*", F.posexplode_outer("items").alias("pos", "elem"))
    .select(
        "id",
        "customer_id",
        "created_at",
        (F.col("pos") + 1).alias("item_no"),
        F.col("elem.qty"),
        F.col("elem.price"),
        F.col("elem.promo"),
    )
)

result = (
    flattened
    .groupBy(
        F.when(F.col("qty") >= 5, "HIGH").otherwise("LOW").alias("qty_tier"),
        F.date_trunc("DAY", "created_at").alias("activity_day"),
        F.when(F.col("price") >= 214, "HIGH").otherwise("LOW").alias("price_tier"),
    )
    .agg(
        F.countDistinct("promo").alias("distinct_promos"),
        F.round(F.sum("price") / F.nullif(F.count(F.when(F.col("promo") == "SPRING10", 1)), F.lit(0)), 2).alias("price_per_spring10"),
        F.round(F.avg("price"), 2).alias("avg_price"),
        F.count(F.when(F.col("promo") == "VIP", 1)).alias("vip_count"),
    )
    .orderBy(F.col("distinct_promos").desc())
)
