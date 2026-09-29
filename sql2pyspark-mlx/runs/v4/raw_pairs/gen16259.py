from pyspark.sql import functions as F

result = (
    df_customers
    .filter((F.col("credit_limit") <= 9600) & ((F.col("signup_date") <= "2025-02-01") | F.col("is_active")))
    .select("id", "signup_date", "age", "name", F.upper("email").alias("email_upper"))
    .orderBy("email_upper")
)
