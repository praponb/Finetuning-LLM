from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("discount") > 0.14)
    .withColumn("channel_prefix", F.substring("channel", 1, 2))
    .orderBy(F.col("amount").desc(), F.col("discount").desc())
)
