from pyspark.sql import functions as F

filtered_customers = (
    df_customers
    .filter((F.col("credit_limit") >= 10600) & ((F.col("country") != "US") | (F.col("credit_limit") <= 9400)))
)

agg = (
    filtered_customers
    .groupBy("is_active")
    .agg(
        F.sum(F.when(F.col("country") == "UK", 1).otherwise(0)).alias("uk_count"),
        F.min("signup_date").alias("min_signup_date"),
    )
)

result = (
    agg
    .orderBy("min_signup_date", F.col("uk_count").desc(), "is_active")
    .limit(10)
)
