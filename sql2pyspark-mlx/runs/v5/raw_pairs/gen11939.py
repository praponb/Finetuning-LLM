from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("shipped_date") <= "2025-05-01")
    .select("shipped_date")
    .union(df_inventory.select("last_restock"))
)
