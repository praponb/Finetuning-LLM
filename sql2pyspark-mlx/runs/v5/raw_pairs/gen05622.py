from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_stores.alias("s"), F.col("s.id") == F.col("ss.store_id"), "inner")
    .filter(F.col("ss.sale_date") >= "2023-04-01")
    .groupBy(F.col("s.city"))
    .agg(F.count("*").alias("num_store_sales"))
    .orderBy(F.col("num_store_sales").desc())
)
