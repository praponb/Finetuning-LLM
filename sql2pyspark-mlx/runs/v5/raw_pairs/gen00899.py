from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("last_restock") <= "2025-04-01")
    .select("last_restock", "product_id")
    .intersect(
        df_inventory
        .filter(F.col("quantity") >= 194)
        .select("last_restock", "product_id")
    )
    .orderBy("product_id")
)
