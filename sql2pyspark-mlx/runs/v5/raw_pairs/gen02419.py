from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("store_id").isin([3, 5]) & (F.col("store_id").isin([2, 5]) | F.col("last_restock").between("2023-03-16", "2024-12-01")))
    .select(
        "id",
        "last_restock",
        F.datediff(F.current_date(), "last_restock").alias("days_since_last_restock"),
        F.coalesce("quantity", F.lit(0)).alias("quantity_filled"),
    )
)
