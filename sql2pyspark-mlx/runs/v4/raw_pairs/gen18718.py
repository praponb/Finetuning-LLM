from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_stores.alias("s"), F.col("s.id") == F.col("ss.store_id"), "left")
    .groupBy(F.col("s.region"))
    .agg(F.round(F.stddev(F.col("ss.units")), 2).alias("stddev_units"))
    .filter(F.col("stddev_units") > 24)
    .orderBy(F.col("stddev_units").desc(), "region")
)
