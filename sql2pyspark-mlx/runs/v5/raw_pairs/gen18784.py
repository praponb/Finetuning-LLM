from pyspark.sql import functions as F

lines = (
    df_carts
    .select("*", F.posexplode("items").alias("pos", "elem"))
    .select(
        "id",
        "created_at",
        "pos",
        F.col("elem.product_id"),
        F.col("elem.qty"),
        F.col("elem.price"),
    )
)

result = (
    lines
    .groupBy(
        F.when(F.col("price") >= 210, "HIGH").otherwise("LOW").alias("price_tier"),
        F.date_trunc("HOUR", "created_at").alias("hour_window"),
    )
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.count("*").alias("event_count"),
        F.count("*").alias("num_rows"),
    )
    .orderBy(F.col("hour_window").desc_nulls_first())
)
