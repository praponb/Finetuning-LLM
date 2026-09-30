from pyspark.sql import functions as F

result = (
    df_customers
    .select(
        "id",
        "signup_date",
        "name",
        "credit_limit",
        F.initcap(F.trim("country")).alias("country_title"),
        F.date_format("signup_date", "yyyy-MM-dd").alias("signup_date_label"),
    )
)
