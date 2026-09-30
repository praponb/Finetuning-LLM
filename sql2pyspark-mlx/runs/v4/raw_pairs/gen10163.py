from pyspark.sql import functions as F

result = (
    df_inventory
    .filter((F.col("last_restock").between("2023-04-01", "2024-04-04") & (F.col("store_id") == 5)) | (F.col("last_restock") > "2023-05-01"))
    .agg(
        F.min("last_restock").alias("min_last_restock"),
        F.sum("quantity").alias("total_quantity"),
    )
    .orderBy("min_last_restock", "total_quantity")
)
