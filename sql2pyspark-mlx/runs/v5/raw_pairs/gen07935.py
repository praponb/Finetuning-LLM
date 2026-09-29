from pyspark.sql import functions as F

result = (
    df_inventory
    .filter((F.col("last_restock") <= "2023-07-01") & (F.year("last_restock") == 2025))
    .select(
        "store_id",
        "quantity",
        F.datediff(F.current_date(), "last_restock").alias("days_since_last_restock"),
    )
)
