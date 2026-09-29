from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.upper(F.trim("name")) == "DAVID LEE")
    .select("signup_date", "credit_limit")
    .orderBy(F.col("credit_limit").desc())
)
