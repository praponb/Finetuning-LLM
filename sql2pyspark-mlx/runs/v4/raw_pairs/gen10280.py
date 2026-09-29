from pyspark.sql import functions as F

result = (
    df_stores.alias("s")
    .join(df_store_sales.alias("ss"), F.col("ss.store_id") == F.col("s.id"), "left")
    .groupBy(F.col("s.city"))
    .agg(F.count(F.col("ss.id")).alias("store_sale_count"))
)
