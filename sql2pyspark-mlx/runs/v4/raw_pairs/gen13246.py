from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_stores.alias("s"), F.col("s.id") == F.col("ss.store_id"), "right")
    .groupBy(F.col("s.region"))
    .agg(F.countDistinct(F.col("ss.revenue")).alias("unique_products"))
)
