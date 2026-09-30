from pyspark.sql import functions as F

result = (
    df_inventory
    .filter((F.col("product_id").isin([4, 10]) & (F.col("quantity") == 154)) | F.col("product_id").isin([4, 9]))
    .select("id", "product_id", "last_restock")
    .orderBy("last_restock")
)
