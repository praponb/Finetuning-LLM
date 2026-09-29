from pyspark.sql import functions as F

result = (
    df_inventory
    .filter((F.col("quantity").isNotNull() & F.col("store_id").isin([1, 4, 5, 6])) | F.col("quantity").isNotNull())
    .select(
        "store_id",
        "product_id",
        "last_restock",
        F.ceil("quantity").alias("quantity_whole"),
    )
    .orderBy("store_id", F.col("quantity_whole").desc())
)
