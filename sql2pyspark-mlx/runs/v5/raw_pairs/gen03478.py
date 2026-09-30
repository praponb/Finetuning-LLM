from pyspark.sql import functions as F

result = (
    df_subscriptions
    .agg(
        F.count("*").alias("total_count"),
        F.sum(F.when(F.col("plan") == "basic", 1).otherwise(0)).alias("basic_count"),
    )
)
