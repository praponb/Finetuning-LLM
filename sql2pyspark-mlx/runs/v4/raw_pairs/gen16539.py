from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("email").like("%@company.co"))
    .select(
        "country",
        "is_active",
        F.last_day("signup_date").alias("signup_date_month_end"),
        F.upper("name").alias("name_upper"),
    )
)
