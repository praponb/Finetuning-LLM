from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(((F.col("last_restock") <= "2024-10-06") & F.col("store_id").isin([2, 3])) | (F.col("last_restock") <= "2025-05-15"))
    .withColumn("last_restock_weekofyear", F.weekofyear("last_restock"))
    .withColumn("quantity_whole", F.ceil("quantity"))
)
