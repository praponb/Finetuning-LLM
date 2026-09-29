from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_stores.alias("s"), F.col("s.id") == F.col("ss.store_id"), "left")
    .filter(F.col("s.store_name").like("A%"))
    .groupBy(F.col("s.city"))
    .agg(F.round(F.stddev(F.col("ss.revenue")), 2).alias("stddev_revenue"))
    .orderBy(F.col("city").desc())
)
