from pyspark.sql import functions as F

store_summary = (
    df_store_sales
    .filter(F.col("store_id").isin([2, 4]))
    .groupBy("store_id")
    .agg(
        F.count("*").alias("store_sale_count"),
        F.min("sale_date").alias("first_sale_date"),
    )
)

result = (
    df_stores.alias("s")
    .join(store_summary.alias("t"), F.col("s.id") == F.col("t.store_id"), "inner")
    .filter(F.col("t.store_sale_count") >= 3)
    .select(F.col("s.region"), F.col("t.store_sale_count"), F.col("t.first_sale_date"))
)
