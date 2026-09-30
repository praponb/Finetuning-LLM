from pyspark.sql import functions as F

result = (
    df_customers
    .filter((F.col("age") <= 36) & ~F.col("city").isin(["Bangkok", "Tokyo", "New York"]))
    .groupBy(F.month("signup_date").alias("signup_month"))
    .agg(
        F.avg("age").alias("avg_age"),
        F.count(F.when(F.col("city") == "New York", 1)).alias("new_york_count"),
    )
)
