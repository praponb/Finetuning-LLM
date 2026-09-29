from pyspark.sql import functions as F

result = (
    df_customers
    .filter((F.col("segment") == "SMB") & F.lower("name").like("%vy %"))
    .groupBy(F.year("signup_date").alias("signup_year"), "segment")
    .agg(
        F.round(F.avg("credit_limit"), 2).alias("avg_credit_limit"),
        F.sum(F.when(F.col("country") == "UK", 1).otherwise(0)).alias("uk_count"),
        F.min("age").alias("min_age"),
    )
)
