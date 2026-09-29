from pyspark.sql import functions as F

result = (
    df_customers
    .filter((F.col("signup_date") > "2025-09-01") & (F.col("is_active") | (F.col("age") >= 57)))
    .select("id", "name", "segment", "credit_limit")
)
