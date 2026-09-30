from pyspark.sql import functions as F

recent_customers = df_customers.filter(F.col("segment").isin(["SMB", "Corporate", "Consumer"]))

customers_stats = (
    recent_customers
    .groupBy("city")
    .agg(
        F.min("age").alias("min_age"),
        F.avg("age").alias("avg_age"),
    )
)

result = (
    customers_stats
    .filter(F.col("avg_age") < 61)
    .orderBy("city")
)
