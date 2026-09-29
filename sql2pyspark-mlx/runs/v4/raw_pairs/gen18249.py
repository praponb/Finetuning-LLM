from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("product_id").isin([1, 3, 4, 8]) & F.col("store_id").isin([4, 5, 6]) & (F.col("product_id") == 5))
    .select(
        "quantity",
        "last_restock",
        "product_id",
        F.date_format("last_restock", "MMM yyyy").alias("last_restock_label"),
    )
)
