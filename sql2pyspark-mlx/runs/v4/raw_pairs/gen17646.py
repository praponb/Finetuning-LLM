from pyspark.sql import functions as F

result = (
    df_products
    .filter(~(F.col("supplier_id").isin([2, 28]) & (F.col("brand") != "Acme")))
    .select("id", "category", "price", F.size("tags").alias("tags_count"))
)
