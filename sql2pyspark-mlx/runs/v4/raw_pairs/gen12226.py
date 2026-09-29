from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy("channel")
    .agg(F.countDistinct("discount").alias("unique_channels"))
    .filter(F.col("unique_channels") < 2)
    .orderBy(F.col("unique_channels").desc())
)
