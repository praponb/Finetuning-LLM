from pyspark.sql import functions as F

store_stats = (
    df_store_sales
    .groupBy("store_id")
    .agg((F.max("units") - F.min("units")).alias("units_range"))
)

result = (
    df_stores.alias("s")
    .join(store_stats.alias("ss"), F.col("s.id") == F.col("ss.store_id"), "inner")
    .select(F.col("s.sqft"), F.col("s.store_name"), F.col("ss.units_range"))
)
