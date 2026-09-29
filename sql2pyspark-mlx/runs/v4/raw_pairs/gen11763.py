from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("signup_date") >= "2025-03-01")
    .select("is_active", F.lower("email").alias("email_lower"))
    .orderBy(F.col("email_lower").desc())
)
