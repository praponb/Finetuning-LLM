from pyspark.sql import functions as F

result = (
    df_products
    .select("price")
    .unionByName(df_store_sales.select(F.col("revenue").alias("price")))
)
