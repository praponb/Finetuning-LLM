from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("product_id").isin([5, 10]))
    .select(
        "last_restock",
        "store_id",
        F.when(F.col("quantity") >= 230, "High").when(F.col("quantity") >= 67, "Medium").otherwise("Low").alias("quantity_tier"),
    )
)
