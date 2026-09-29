from pyspark.sql import functions as F

result = (
    df_carts
    .select("*", F.posexplode_outer("items").alias("pos", "entry"))
    .select(
        "id",
        "customer_id",
        "created_at",
        "pos",
        F.col("entry.product_id"),
        F.col("entry.price"),
        F.col("entry.promo"),
    )
)
