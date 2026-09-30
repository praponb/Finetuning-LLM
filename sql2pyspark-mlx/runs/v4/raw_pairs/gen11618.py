from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("store_id") == 3)
    .groupBy(
        F.year("last_restock").alias("last_restock_year"),
        "product_id",
        "store_id",
    )
    .agg(
        F.min("last_restock").alias("min_last_restock"),
        F.min("quantity").alias("min_quantity"),
        (F.max("quantity") - F.min("quantity")).alias("quantity_range"),
    )
)
