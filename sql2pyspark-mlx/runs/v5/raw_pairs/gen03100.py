from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_stores.filter(F.lower("store_name").like("%irp%")).alias("s"), F.col("ss.store_id") == F.col("s.id"), "left_anti")
    .filter(F.col("ss.store_id").isin([1, 4]))
    .select(F.col("ss.id"), F.col("ss.store_id"))
)
