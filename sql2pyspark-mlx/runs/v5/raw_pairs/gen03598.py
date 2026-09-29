from pyspark.sql import functions as F

filtered_customers = (
    df_customers
    .filter(F.col("email").like("%@gmail.com") & (F.col("credit_limit") < 8300) & F.col("segment").isNotNull())
)

customers_stats = (
    filtered_customers
    .groupBy("segment")
    .agg(
        F.sum("age").alias("total_age"),
        F.round(((100 * F.sum(F.when(F.col("city") == "New York", 1).otherwise(0))) / F.count("*")), 1).alias("pct_new_york"),
    )
)

result = (
    customers_stats
    .filter(F.col("pct_new_york") < 79)
    .orderBy("segment")
)
