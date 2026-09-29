from pyspark.sql import functions as F

result = (
    df_inventory
    .filter((F.col("product_id").isin([3, 4, 6, 9]) & (F.col("quantity") == 164)) | F.col("store_id").isin([2, 5]))
    .select(
        "id",
        "quantity",
        F.ceil("quantity").alias("quantity_whole"),
        F.add_months("last_restock", 1).alias("last_restock_plus_months"),
    )
)
