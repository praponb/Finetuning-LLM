from pyspark.sql import functions as F

result = (
    df_customers
    .filter((F.col("signup_date") < "2023-07-01") & F.col("signup_date").between("2023-08-05", "2024-06-01"))
    .groupBy("city", "segment")
    .agg(
        F.max("credit_limit").alias("max_credit_limit"),
        F.count("credit_limit").alias("credit_limit_count"),
    )
    .filter(F.col("credit_limit_count") < 2)
    .orderBy("city", "segment")
    .limit(1)
)
