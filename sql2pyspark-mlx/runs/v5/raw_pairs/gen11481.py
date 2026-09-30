from pyspark.sql import functions as F

result = (
    df_orders
    .alias("src")
    .join(df_stores.alias("dim"), F.col("src.store_id") == F.col("dim.id"), "inner")
    .groupBy(F.col("dim.region").alias("region"), F.col("src.status").alias("status"))
    .agg(
        F.count("*").alias("num_rows"),
        F.max(F.col("src.shipping_fee")).alias("max_shipping_fee"),
        F.max(F.col("src.discount")).alias("max_discount"),
    )
    .orderBy(F.col("region").desc())
)
