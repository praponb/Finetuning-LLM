from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy("channel")
    .agg(F.round(F.stddev("amount"), 2).alias("stddev_amount"))
    .filter(F.col("stddev_amount") < 63)
    .orderBy(F.col("stddev_amount").desc())
)
