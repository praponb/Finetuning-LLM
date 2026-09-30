from pyspark.sql import functions as F

result = (
    df_inventory
    .select(
        "last_restock",
        "store_id",
        F.col("quantity").cast("string").alias("quantity_str"),
        F.month("last_restock").alias("last_restock_month"),
    )
    .orderBy(F.col("last_restock").desc())
)
