from pyspark.sql import functions as F

store_summary = (
    df_store_sales
    .filter(F.col("units") <= 15)
    .groupBy("store_id")
    .agg(
        F.round(F.stddev("units"), 2).alias("stddev_units"),
        F.countDistinct("units").alias("unique_stores"),
    )
)

result = (
    df_stores.alias("s")
    .join(store_summary.alias("ss"), F.col("s.id") == F.col("ss.store_id"), "inner")
    .select(
        F.col("s.id"),
        F.col("s.store_name"),
        F.col("ss.stddev_units"),
        F.col("ss.unique_stores"),
    )
    .orderBy("id", F.col("stddev_units").desc())
)
