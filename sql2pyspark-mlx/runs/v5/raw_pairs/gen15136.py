from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.year("last_restock") == 2024)
    .select("last_restock")
    .intersect(
        df_inventory
        .filter(F.col("last_restock") < "2025-01-01")
        .select("last_restock")
    )
)
