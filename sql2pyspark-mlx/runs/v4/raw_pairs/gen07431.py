from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_stores.alias("s"), F.col("s.id") == F.col("o.store_id"), "inner")
    .groupBy(F.col("s.region"))
    .agg(
        F.sum(F.col("o.discount")).alias("total_discount"),
        F.count("*").alias("num_orders"),
    )
)
