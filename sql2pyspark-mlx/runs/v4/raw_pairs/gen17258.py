from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy("channel")
    .agg(F.min("discount").alias("min_discount"))
    .orderBy(F.col("min_discount").desc(), "channel")
    .limit(3)
)
