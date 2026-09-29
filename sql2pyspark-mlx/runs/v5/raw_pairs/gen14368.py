from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_stores.filter(F.col("sqft") == 11368).alias("s"), F.col("ss.store_id") == F.col("s.id"), "left_semi")
    .filter(F.col("ss.revenue") >= 3300)
    .select(F.col("ss.id"), F.col("ss.store_id"))
    .orderBy(F.col("store_id").desc(), F.col("id").desc())
)
