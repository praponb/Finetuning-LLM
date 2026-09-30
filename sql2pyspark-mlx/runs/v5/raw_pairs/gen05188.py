from pyspark.sql import functions as F

result = (
    df_stores.alias("s")
    .join(df_store_sales.alias("ss"), F.col("s.id") == F.col("ss.store_id"), "left")
    .select(F.col("s.region"), F.col("ss.units"))
    .orderBy(F.col("units").desc(), "region")
)
