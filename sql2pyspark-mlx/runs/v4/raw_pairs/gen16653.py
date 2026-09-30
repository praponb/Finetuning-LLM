from pyspark.sql import functions as F

result = (
    df_products
    .filter(~((F.col("cost") < 500) & F.array_contains("tags", "premium")))
    .select("id", "product_name", F.col("price").cast("int").alias("price_int"))
)
