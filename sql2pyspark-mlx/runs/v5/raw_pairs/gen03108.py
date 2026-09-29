from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.month("signup_date").isin([1, 6, 10]))
    .select(
        "id",
        "credit_limit",
        "is_active",
        "name",
        F.ceil("credit_limit").alias("credit_limit_whole"),
        F.concat("segment", F.lit(" "), "email").alias("segment_email"),
    )
)
