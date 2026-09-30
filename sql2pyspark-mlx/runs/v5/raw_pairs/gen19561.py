from pyspark.sql import functions as F

result = (
    df_stores.alias("s")
    .join(df_inventory.alias("i"), F.col("s.id") == F.col("i.store_id"), "left")
    .select(
        F.col("s.sqft"),
        F.col("s.region"),
        F.col("s.store_name"),
        F.col("i.quantity"),
    )
)
