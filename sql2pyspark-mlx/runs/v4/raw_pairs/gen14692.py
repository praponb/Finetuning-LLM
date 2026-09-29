from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("store_id").isin([1, 5]))
    .select(
        "id",
        "last_restock",
        "product_id",
        F.when(F.col("quantity") >= 216, "High").when(F.col("quantity") >= 37, "Medium").otherwise("Low").alias("quantity_level"),
    )
)
