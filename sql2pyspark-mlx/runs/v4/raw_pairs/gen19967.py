from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("quantity") <= 175)
    .select("last_restock")
    .subtract(
        df_inventory
        .filter(F.col("store_id").isin([3, 6]))
        .select("last_restock")
    )
    .orderBy("last_restock")
)
