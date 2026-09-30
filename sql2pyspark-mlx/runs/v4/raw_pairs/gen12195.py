from pyspark.sql import functions as F

result = (
    df_customers
    .select(
        "city",
        "signup_date",
        "is_active",
        F.concat_ws("-", "email", "name").alias("email_name_key"),
        F.last_day("signup_date").alias("signup_date_month_end"),
    )
)
