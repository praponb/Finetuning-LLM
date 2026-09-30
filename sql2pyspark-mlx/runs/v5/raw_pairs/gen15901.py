from pyspark.sql import functions as F

lines = df_carts.select("id", "customer_id", "created_at", F.inline("items"))

result = (
    lines
    .select("id", "created_at", "product_id", "qty", "price")
    .orderBy(F.col("product_id").desc(), "qty")
)
