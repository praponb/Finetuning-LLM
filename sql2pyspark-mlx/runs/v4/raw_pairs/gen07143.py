from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("signup_date") >= "2025-05-23")
    .select("id", "segment", "signup_date")
)
