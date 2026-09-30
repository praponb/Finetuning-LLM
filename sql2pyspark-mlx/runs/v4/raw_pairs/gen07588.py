from pyspark.sql import functions as F

result = (
    df_carts
    .filter(F.col("customer_id").isin([7, 12, 25, 36]))
    .select(
        "id",
        "customer_id",
        F.size("items").alias("num_items"),
        F.col("attributes").getItem("utm_source").alias("utm_source"),
        F.col("attributes").getItem("weight_kg").try_cast("double").alias("weight_kg"),
        F.exists("items", lambda x: x["price"] > 100).alias("has_premium_item"),
    )
)
