from pyspark.sql import functions as F

store_stats = (
    df_store_sales
    .groupBy("store_id")
    .agg(
        F.count("*").alias("num_store_sales"),
        F.min("sale_date").alias("min_sale_date"),
    )
)

result = (
    df_stores.alias("s")
    .join(store_stats.alias("t"), F.col("s.id") == F.col("t.store_id"), "left")
    .filter(F.col("t.num_store_sales") >= 9)
    .select(F.col("s.region"), F.col("t.num_store_sales"), F.col("t.min_sale_date"))
    .orderBy("min_sale_date")
)
