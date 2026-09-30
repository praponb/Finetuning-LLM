from pyspark.sql import functions as F

exploded = df_carts.select("id", "customer_id", "created_at", F.inline("items"))

result = (
    exploded
    .select("id", "customer_id", "created_at", "product_id", "price")
    .orderBy(F.col("price").desc_nulls_last(), F.col("id").desc())
)
