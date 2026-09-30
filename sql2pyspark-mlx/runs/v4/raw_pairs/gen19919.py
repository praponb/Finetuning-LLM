from pyspark.sql import functions as F

result = (
    df_customers
    .filter((F.col("signup_date") < "2025-01-09") & ~F.col("segment").isin(["SMB", "Corporate", "Consumer"]))
    .groupBy(F.date_trunc("MONTH", "signup_date").alias("signup_month"))
    .agg(
        F.count("*").alias("num_customers"),
        F.sum("age").alias("total_age"),
    )
    .orderBy(F.col("num_customers").desc(), F.col("total_age").desc())
)
