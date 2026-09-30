from pyspark.sql import functions as F

result = (
    df_subscriptions
    .agg(F.count("*").alias("row_count"))
    .orderBy(F.col("row_count").desc())
)
