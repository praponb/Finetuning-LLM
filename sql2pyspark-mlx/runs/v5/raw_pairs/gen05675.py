from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("quantity") < 170)
    .select("last_restock", "product_id")
    .subtract(
        df_inventory
        .filter(F.col("last_restock").between("2024-04-02", "2025-09-07"))
        .select("last_restock", "product_id")
    )
)
