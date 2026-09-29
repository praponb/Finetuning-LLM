from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_stores.alias("s"), F.col("ss.store_id") == F.col("s.id"), "left")
    .filter((F.col("s.city") == "Chiang Mai") | (F.col("ss.units") == 38))
    .select(F.col("ss.id"), F.col("ss.units"), F.col("s.store_name"))
)
