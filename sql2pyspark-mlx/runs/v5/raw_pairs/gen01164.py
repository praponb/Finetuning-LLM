from pyspark.sql import functions as F

result = (
    df_carts
    .withColumn("rec", F.explode("items"))
    .select(
        "id",
        "customer_id",
        "created_at",
        F.col("rec.qty"),
        F.col("rec.price"),
        F.col("rec.promo"),
    )
)
