from pyspark.sql import functions as F

result = (
    df_inventory
    .filter((F.col("product_id") == 1) & F.col("last_restock").between("2023-09-01", "2025-08-30"))
    .select(
        "product_id",
        "store_id",
        "last_restock",
        F.last_day("last_restock").alias("last_restock_month_end"),
    )
)
