from pyspark.sql import functions as F

store_summary = (
    df_store_sales
    .filter(F.col("store_id").isin([3, 4, 5, 6]))
    .groupBy("store_id")
    .agg(
        F.count("units").alias("units_count"),
        F.min("units").alias("min_units"),
    )
)

result = (
    df_stores.alias("s")
    .join(store_summary.alias("ss"), F.col("s.id") == F.col("ss.store_id"), "left")
    .select(
        F.col("s.id"),
        F.col("s.store_name"),
        F.col("ss.units_count"),
        F.col("ss.min_units"),
    )
    .orderBy("store_name")
)
