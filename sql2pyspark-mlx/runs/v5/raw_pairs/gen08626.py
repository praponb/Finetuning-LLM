from pyspark.sql import functions as F

filtered_customers = df_customers.filter(F.col("name").like("Gr%") | F.lower("email").like("%ran%"))

customers_stats = (
    filtered_customers
    .groupBy(
        "segment",
        F.when(F.col("credit_limit") >= 16500, "high").when(F.col("credit_limit") >= 5100, "mid").otherwise("low").alias("credit_limit_bucket"),
    )
    .agg(
        F.min("age").alias("min_age"),
        F.round(F.avg("age"), 2).alias("avg_age"),
    )
)

result = (
    customers_stats
    .select("segment", "credit_limit_bucket", "min_age", "avg_age")
    .orderBy(F.col("segment").desc(), F.col("min_age").desc(), "credit_limit_bucket")
    .limit(10)
)
