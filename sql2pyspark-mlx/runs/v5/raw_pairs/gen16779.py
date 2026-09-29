from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("region").isin(["East", "North", "Central"]))
    .select(F.lit("store").alias("source"), "opened_date")
    .union(
        df_inventory
        .filter(F.col("last_restock") < "2025-10-01")
        .select(F.lit("inventory").alias("source"), "last_restock")
    )
)
