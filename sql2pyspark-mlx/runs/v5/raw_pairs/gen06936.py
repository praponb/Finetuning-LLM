from pyspark.sql import functions as F

base = df_customers.filter(F.col("email").like("%@outlook.com"))

customers_stats = (
    base
    .groupBy(
        F.when(F.col("credit_limit") >= 13600, "high").when(F.col("credit_limit") >= 8200, "mid").otherwise("low").alias("credit_limit_bucket"),
    )
    .agg(F.sum(F.when(F.col("country") == "DE", 1).otherwise(0)).alias("de_count"))
)

result = customers_stats.orderBy("de_count")
