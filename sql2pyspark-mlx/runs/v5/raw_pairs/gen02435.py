from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_stores.alias("s"), F.col("s.id") == F.col("i.store_id"), "inner")
    .select(
        F.col("i.id"),
        F.col("i.quantity"),
        F.col("s.sqft"),
        F.col("s.region"),
        F.col("s.store_name"),
    )
)
