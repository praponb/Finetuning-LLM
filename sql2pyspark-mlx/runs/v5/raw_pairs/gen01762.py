from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_stores.alias("s"), F.col("ss.store_id") == F.col("s.id"), "inner")
    .filter(F.col("ss.revenue").between(1900, 2100))
    .groupBy(F.year(F.col("s.opened_date")).alias("opened_year"), F.col("s.region"))
    .agg(
        F.count("*").alias("store_sale_count"),
        F.count("*").alias("total_count"),
    )
    .orderBy("total_count")
)
