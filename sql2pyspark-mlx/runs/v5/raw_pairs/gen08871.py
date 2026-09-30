from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("last_restock").between("2023-05-01", "2025-01-01") & F.col("product_id").isin([2, 5]))
    .select(
        "product_id",
        "last_restock",
        F.last_day("last_restock").alias("last_restock_month_end"),
        F.last_day("last_restock").alias("last_restock_month_end_2"),
    )
    .orderBy("last_restock_month_end_2")
)
