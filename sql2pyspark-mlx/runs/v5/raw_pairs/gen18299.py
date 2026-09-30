from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("last_restock").between("2024-09-01", "2025-03-01") & (F.col("store_id") == 5))
    .select(
        "id",
        "product_id",
        "quantity",
        F.round("quantity", 1).alias("quantity_rounded"),
    )
)
