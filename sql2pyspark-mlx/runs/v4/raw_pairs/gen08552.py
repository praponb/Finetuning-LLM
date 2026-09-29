from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.year("signup_date") == 2024)
    .select(
        "id",
        "is_active",
        "credit_limit",
        "country",
        F.concat("email", F.lit(" "), "city").alias("email_city"),
    )
)
