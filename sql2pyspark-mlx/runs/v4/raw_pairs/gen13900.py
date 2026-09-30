from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("credit_limit").between(4500, 9100) & (F.col("signup_date") <= "2024-07-24"))
    .select(
        "name",
        "email",
        "age",
        F.datediff(F.current_date(), "signup_date").alias("days_since_signup_date"),
    )
)
