from pyspark.sql import functions as F

result = (
    df_customers
    .select(
        "id",
        "is_active",
        F.datediff(F.current_date(), "signup_date").alias("days_since_signup_date"),
    )
)
