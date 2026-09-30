from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("credit_limit").between(5600, 7400) & F.col("signup_date").between("2024-01-01", "2025-03-01"))
)
