from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy(
        F.when(F.col("credit_limit") >= 15000, "LARGE").otherwise("SMALL").alias("credit_limit_tier"),
    )
    .agg(
        F.count(F.when(F.col("city") == "London", 1)).alias("london_rows"),
        F.stddev_samp("credit_limit").alias("credit_limit_stddev"),
        F.count(F.when(F.col("city") == "Tokyo", 1)).alias("tokyo_rows"),
        F.avg(F.when(F.col("country") == "US", F.col("credit_limit"))).alias("us_avg_credit_limit"),
    )
)
