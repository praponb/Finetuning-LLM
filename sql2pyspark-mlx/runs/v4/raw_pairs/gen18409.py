from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("store_id").isin([1, 6]))
    .select(
        "store_id",
        "last_restock",
        "product_id",
        F.round("quantity", 1).alias("quantity_rounded"),
        F.date_trunc("MONTH", "last_restock").alias("last_restock_period"),
    )
)
