from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("quantity").between(168, 220) & (F.col("quantity") < 148))
    .select(
        "id",
        "store_id",
        "last_restock",
        "product_id",
        F.col("quantity").cast("string").alias("quantity_str"),
    )
)
