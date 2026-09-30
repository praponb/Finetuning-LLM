from pyspark.sql import functions as F

result = (
    df_customers
    .filter((F.col("signup_date") >= "2025-06-01") & (F.col("segment") != "SMB"))
)
