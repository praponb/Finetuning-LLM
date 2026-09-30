from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("signup_date") > "2025-09-10")
    .groupBy(
        F.when(F.col("credit_limit") >= 16900, "high").when(F.col("credit_limit") >= 7100, "mid").otherwise("low").alias("credit_limit_bucket"),
    )
    .agg(F.sum("credit_limit").alias("total_credit_limit"))
    .filter(F.col("total_credit_limit") < 44000)
    .select("credit_limit_bucket", "total_credit_limit")
)
