from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_stores.alias("s"), F.col("i.store_id") == F.col("s.id"), "inner")
    .filter(F.col("s.sqft") <= 10454)
    .select(
        F.col("i.id"),
        F.col("i.product_id"),
        F.col("i.store_id"),
        F.col("s.store_name"),
        F.col("s.region"),
        F.col("s.sqft"),
        F.last_day(F.col("i.last_restock")).alias("last_restock_month_end"),
    )
)
