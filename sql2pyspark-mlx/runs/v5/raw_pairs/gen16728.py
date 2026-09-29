from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("credit_limit").isNull())
    .select(
        "id",
        "age",
        "signup_date",
        "is_active",
        F.substring("email", 1, 3).alias("email_prefix"),
    )
)
