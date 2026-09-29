from pyspark.sql import functions as F

exploded = (
    df_carts
    .withColumn("line", F.explode_outer("items"))
    .select(
        "id",
        "created_at",
        F.col("line.product_id"),
        F.col("line.qty"),
        F.col("line.price"),
    )
)

result = (
    exploded
    .select("created_at", "product_id", "qty", "price")
    .orderBy(F.col("product_id").desc(), F.col("qty").desc())
)
