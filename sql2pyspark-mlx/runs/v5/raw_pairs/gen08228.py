from pyspark.sql import functions as F

result = (
    df_inventory
    .filter((F.year("last_restock") == 2024) & F.col("store_id").isin([2, 6]))
    .select(
        "last_restock",
        "quantity",
        "store_id",
        F.datediff(F.current_date(), "last_restock").alias("days_since_last_restock"),
    )
    .orderBy("last_restock", F.col("quantity").desc())
)
