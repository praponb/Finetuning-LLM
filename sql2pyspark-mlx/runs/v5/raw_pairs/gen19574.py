from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("store_id").isin([3, 4, 5]))
    .select("last_restock")
    .subtract(
        df_inventory
        .filter(F.year("last_restock") >= 2023)
        .select("last_restock")
    )
)
