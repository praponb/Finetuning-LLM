from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("last_restock").between("2023-06-28", "2024-12-01") & F.col("last_restock").between("2023-04-01", "2025-05-28"))
    .select(
        "id",
        "quantity",
        F.last_day("last_restock").alias("last_restock_month_end"),
    )
)
