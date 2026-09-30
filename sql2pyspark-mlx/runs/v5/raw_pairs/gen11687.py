from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_stores.alias("s"), F.col("ss.store_id") == F.col("s.id"), "inner")
    .groupBy(F.col("s.city"), F.col("s.region"))
    .agg(F.max(F.col("ss.sale_date")).alias("last_sale_date"))
    .orderBy("last_sale_date")
)
