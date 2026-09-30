from pyspark.sql import functions as F

lines = (
    df_carts
    .select("*", F.posexplode_outer("items").alias("pos", "item"))
    .select(
        "id",
        "pos",
        F.col("item.product_id"),
        F.col("item.qty"),
        F.col("item.promo"),
    )
)

result = (
    lines
    .groupBy("promo")
    .agg(
        F.sum(F.when(F.col("promo") == "WELCOME5", F.col("pos")).otherwise(0)).alias("welcome5_pos"),
        F.round(F.avg("product_id"), 2).alias("avg_product_id"),
        F.count(F.when(F.col("promo") == "WELCOME5", 1)).alias("welcome5_count"),
    )
    .orderBy(F.col("welcome5_pos").asc_nulls_last())
)
