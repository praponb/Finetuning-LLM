from pyspark.sql import functions as F

store_summary = (
    df_store_sales
    .groupBy("store_id")
    .agg(
        F.min("units").alias("min_units"),
        F.count("*").alias("store_sale_count"),
    )
)

result = (
    df_stores.alias("s")
    .join(store_summary.alias("t"), F.col("s.id") == F.col("t.store_id"), "left")
    .select(F.col("s.sqft"), F.col("t.min_units"), F.col("t.store_sale_count"))
)
