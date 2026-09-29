from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy("city")
    .agg(
        F.sum(F.when(F.col("city") == "Singapore", 1).otherwise(0)).alias("singapore_count"),
        F.count("credit_limit").alias("credit_limit_count"),
        F.max("credit_limit").alias("max_credit_limit"),
    )
    .orderBy(F.col("credit_limit_count").desc(), "city")
    .limit(5)
)
