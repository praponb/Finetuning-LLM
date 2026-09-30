from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_stores.alias("s"), F.col("i.store_id") == F.col("s.id"), "inner")
    .filter(F.month(F.col("s.opened_date")).isin([1, 5, 7]) & (F.col("i.quantity") <= 77))
    .select(F.col("i.id"), F.col("i.store_id"), F.col("s.sqft"), F.col("s.store_name"))
)
