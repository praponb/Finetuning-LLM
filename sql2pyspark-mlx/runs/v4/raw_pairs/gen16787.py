from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "inner")
    .groupBy(F.col("s.city"))
    .agg(
        F.countDistinct(F.col("o.amount")).alias("unique_statuss"),
        F.round(F.stddev(F.col("o.discount")), 2).alias("stddev_discount"),
    )
)
