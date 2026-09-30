from pyspark.sql import functions as F

base = df_customers.filter(F.col("is_active"))

customers_stats = (
    base
    .groupBy("segment")
    .agg(
        F.count(F.when(F.col("country") == "UK", 1)).alias("uk_count"),
        F.count("credit_limit").alias("credit_limit_count"),
    )
)

result = (
    customers_stats
    .select("segment", "uk_count")
    .orderBy(F.col("uk_count").desc(), F.col("segment").desc())
)
