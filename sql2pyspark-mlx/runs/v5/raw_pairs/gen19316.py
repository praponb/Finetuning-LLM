from pyspark.sql import functions as F

exploded = (
    df_carts
    .select("*", F.posexplode("items").alias("pos", "line"))
    .select(
        "id",
        "customer_id",
        "created_at",
        "coupon_codes",
        "pos",
        F.col("line.product_id"),
        F.col("line.price"),
    )
)

result = (
    exploded
    .groupBy("product_id")
    .agg(
        F.max("created_at").alias("ended_at"),
        F.round(F.avg("pos"), 2).alias("avg_pos"),
        F.max("created_at").alias("last_seen"),
    )
    .orderBy(F.col("avg_pos").desc_nulls_last())
)
