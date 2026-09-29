from pyspark.sql import functions as F

flattened = (
    df_carts
    .select("*", F.posexplode("items").alias("pos", "entry"))
    .select(
        "id",
        "customer_id",
        "created_at",
        (F.col("pos") + 1).alias("position"),
        F.col("entry.product_id"),
        F.col("entry.qty"),
        F.col("entry.price"),
        F.col("entry.promo"),
    )
)

result = (
    flattened
    .groupBy("product_id")
    .agg(
        F.max("created_at").alias("ended_at"),
        F.min("created_at").alias("started_at"),
    )
    .orderBy(F.col("product_id").desc())
)
