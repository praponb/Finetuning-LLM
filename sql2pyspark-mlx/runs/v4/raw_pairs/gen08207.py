from pyspark.sql import functions as F

valid_customers = (
    df_customers
    .filter((F.col("age") == 40) & F.col("name").like("Ka%") & (F.upper(F.trim("email")) == "IVY@GMAIL.COM"))
    .withColumn("age_plus_credit_limit", (F.col("age") + F.col("credit_limit")))
)

summary = (
    valid_customers
    .groupBy(F.date_trunc("MONTH", "signup_date").alias("signup_month"))
    .agg(
        F.count("credit_limit").alias("credit_limit_count"),
        F.sum("age_plus_credit_limit").alias("total_age_plus_credit_limit"),
    )
)

result = (
    summary
    .filter(F.col("credit_limit_count") < 6)
    .select("signup_month", "credit_limit_count", "total_age_plus_credit_limit")
    .orderBy("signup_month")
    .limit(20)
)
