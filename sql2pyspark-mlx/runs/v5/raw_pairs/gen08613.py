from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("is_active") & (F.col("credit_limit") > 14000) & F.col("city").isNull())
    .select(
        "country",
        "signup_date",
        "city",
        (F.col("age") + F.col("credit_limit")).alias("age_plus_credit_limit"),
        F.ceil("credit_limit").alias("credit_limit_whole"),
    )
)
