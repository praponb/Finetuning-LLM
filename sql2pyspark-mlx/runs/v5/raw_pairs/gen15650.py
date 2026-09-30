from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("store_id").isin([1, 4, 5, 6]))
    .select(
        "id",
        "product_id",
        "last_restock",
        "store_id",
        F.col("quantity").cast("double").alias("quantity_num"),
    )
    .orderBy(F.col("product_id").desc())
)
