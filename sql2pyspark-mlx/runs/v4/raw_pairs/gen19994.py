from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("quantity") <= 67)
    .select(
        "product_id",
        "store_id",
        F.col("quantity").cast("string").alias("quantity_str"),
    )
)
