from pyspark.sql import functions as F

store_stats = (
    df_store_sales
    .groupBy("store_id")
    .agg(F.max("sale_date").alias("last_sale_date"))
)

result = (
    df_stores.alias("s")
    .join(store_stats.alias("t"), F.col("s.id") == F.col("t.store_id"), "left")
    .select(F.col("s.id"), F.col("s.sqft"), F.col("s.city"), F.col("t.last_sale_date"))
    .orderBy("city")
)
