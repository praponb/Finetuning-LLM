from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "inner")
    .groupBy(F.col("s.region"))
    .agg(
        F.avg(F.col("o.amount")).alias("avg_amount"),
        F.sum(F.when(F.col("o.channel") == "mobile", 1).otherwise(0)).alias("mobile_count"),
    )
)
