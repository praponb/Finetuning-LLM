from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy(F.date_trunc("MONTH", "signup_date").alias("signup_month"))
    .agg(
        F.round(((100 * F.sum(F.when(F.col("country") == "JP", 1).otherwise(0))) / F.count("*")), 1).alias("pct_jp"),
        F.sum(F.when(F.col("city") == "New York", F.col("credit_limit")).otherwise(0)).alias("new_york_credit_limit"),
        F.sum("credit_limit").alias("total_credit_limit"),
        F.count("age").alias("age_count"),
    )
    .filter(F.col("age_count") >= 4)
    .drop("age_count")
)
