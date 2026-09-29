from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy("channel")
    .agg(F.countDistinct("amount").alias("unique_channels"))
    .orderBy("unique_channels")
)
