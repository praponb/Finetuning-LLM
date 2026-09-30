from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_stores.alias("s"), F.col("s.id") == F.col("ss.store_id"), "left")
    .filter(F.col("s.store_name").isNotNull())
    .select(F.col("ss.units"), F.col("s.sqft"))
)
