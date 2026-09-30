from pyspark.sql import functions as F

store_stats = (
    df_store_sales
    .groupBy("store_id")
    .agg(
        F.sum("units").alias("total_units"),
        F.round(F.stddev("units"), 2).alias("stddev_units"),
    )
)

result = (
    df_stores.alias("s")
    .join(store_stats.alias("ss"), F.col("s.id") == F.col("ss.store_id"), "inner")
    .filter(F.col("ss.stddev_units") > 70)
    .select(
        F.col("s.id"),
        F.col("s.store_name"),
        F.col("s.region"),
        F.col("ss.total_units"),
        F.col("ss.stddev_units"),
    )
    .orderBy(F.col("total_units").desc(), "id")
)
