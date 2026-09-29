from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_stores.alias("s"), F.col("ss.store_id") == F.col("s.id"), "inner")
    .groupBy(F.col("s.city"))
    .agg(F.count("*").alias("num_store_sales"))
    .orderBy(F.col("num_store_sales").desc(), F.col("city").desc())
    .limit(50)
)
