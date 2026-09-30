from pyspark.sql import functions as F

result = (
    df_products
    .groupBy("brand", "supplier_id")
    .pivot("category", ["Fashion", "Books"])
    .agg(F.sum("price").alias("amount"), F.count(F.lit(1)).alias("item_count"))
)
