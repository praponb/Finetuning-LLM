from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("store_id").isin([2, 3, 5]))
    .select(
        "product_id",
        "store_id",
        F.datediff(F.current_date(), "last_restock").alias("days_since_last_restock"),
    )
)
