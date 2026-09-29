from pyspark.sql import functions as F

result = (
    df_customers
    .select(
        "id",
        "credit_limit",
        "signup_date",
        "email",
        F.coalesce("email", F.lit("N/A")).alias("email_filled"),
    )
    .orderBy(F.col("id").desc(), F.col("email_filled").desc())
    .limit(20)
)
