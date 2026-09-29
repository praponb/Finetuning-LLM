from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("segment") == "Corporate")
    .groupBy("segment", F.month("signup_date").alias("signup_month"))
    .agg(F.avg("credit_limit").alias("avg_credit_limit"))
    .filter(F.col("avg_credit_limit") >= 10700)
    .orderBy("avg_credit_limit", "segment", "signup_month")
    .limit(10)
)
