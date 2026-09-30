from pyspark.sql import functions as F

result = (
    df_inventory
    .filter((F.col("quantity").between(62, 177) & (F.year("last_restock") == 2024)) | F.col("store_id").isin([3, 4]))
    .select(
        "id",
        "last_restock",
        "store_id",
        F.month("last_restock").alias("last_restock_month"),
    )
    .orderBy("last_restock_month")
)
