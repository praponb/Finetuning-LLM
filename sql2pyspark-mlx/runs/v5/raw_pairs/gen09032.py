from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_stores.alias("s"), F.col("ss.store_id") == F.col("s.id"), "inner")
    .filter(F.year(F.col("ss.sale_date")) >= 2025)
    .groupBy(F.col("s.region"))
    .agg(F.round(F.avg(F.col("ss.units")), 2).alias("avg_units"))
    .filter(F.col("avg_units") >= 73)
    .orderBy(F.col("avg_units").desc())
)
