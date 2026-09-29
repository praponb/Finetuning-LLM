from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy("segment")
    .agg(
        F.sum(F.when(F.col("country") == "UK", 1).otherwise(0)).alias("uk_count"),
        F.avg("credit_limit").alias("avg_credit_limit"),
        F.max("credit_limit").alias("max_credit_limit"),
    )
)
