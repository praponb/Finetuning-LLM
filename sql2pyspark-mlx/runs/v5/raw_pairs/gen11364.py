from pyspark.sql import functions as F

valid_customers = (
    df_customers
    .filter(~F.col("email").like("%@gmail.com") | ~F.col("name").like("E%"))
    .withColumn("segment_group", F.when(F.col("segment").isin(["Consumer", "Corporate"]), "Tier 1").otherwise("Other"))
)

customers_stats = (
    valid_customers
    .groupBy(F.month("signup_date").alias("signup_month"))
    .agg(F.count(F.when(F.col("city") == "Tokyo", 1)).alias("tokyo_count"))
)

result = (
    customers_stats
    .orderBy(F.col("signup_month").desc())
    .limit(10)
)
