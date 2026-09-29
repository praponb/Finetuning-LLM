from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_stores.alias("s"), F.col("s.id") == F.col("ss.store_id"), "inner")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "left")
    .filter(F.col("s.store_name").like("%Town%"))
    .groupBy(F.col("s.region"))
    .agg(
        F.avg(F.col("ss.units")).alias("avg_units"),
        F.count("*").alias("store_sale_count"),
    )
    .orderBy(F.col("store_sale_count").desc(), "avg_units")
)
