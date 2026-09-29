from pyspark.sql import functions as F

result = (
    df_inventory
    .select(
        "id",
        "last_restock",
        "store_id",
        "product_id",
        F.when(F.col("quantity") >= 244, "High").when(F.col("quantity") >= 106, "Medium").otherwise("Low").alias("quantity_band"),
    )
)
