from pyspark.sql import functions as F

store_stats = (
    df_store_sales
    .filter(F.col("units").between(13, 16))
    .groupBy("store_id")
    .agg(
        F.max("sale_date").alias("last_sale_date"),
        F.max("units").alias("max_units"),
    )
)

result = (
    df_stores.alias("s")
    .join(store_stats.alias("ss"), F.col("s.id") == F.col("ss.store_id"), "left")
    .select(
        F.col("s.id"),
        F.col("s.opened_date"),
        F.col("s.city"),
        F.col("ss.last_sale_date"),
        F.col("ss.max_units"),
    )
)
