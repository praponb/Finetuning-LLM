from pyspark.sql import functions as F

valid_customers = (
    df_customers
    .filter(F.col("credit_limit").between(5500, 15500) & (F.col("city").isin(["London", "Berlin"]) | (F.col("segment") == "Corporate")))
)

agg = (
    valid_customers
    .groupBy(
        F.when(F.col("credit_limit") >= 17300, "high").when(F.col("credit_limit") >= 7400, "mid").otherwise("low").alias("credit_limit_bucket"),
    )
    .agg(
        F.max("signup_date").alias("latest_signup_date"),
        F.min("credit_limit").alias("min_credit_limit"),
    )
)

result = agg.orderBy("latest_signup_date")
