from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_stores.filter(F.col("city").isin(["Berlin", "Tokyo", "Chiang Mai"])).alias("s"), F.col("ss.store_id") == F.col("s.id"), "left_semi")
    .select(F.col("ss.id"), F.col("ss.store_id"))
    .orderBy(F.col("id").desc())
    .limit(20)
)
