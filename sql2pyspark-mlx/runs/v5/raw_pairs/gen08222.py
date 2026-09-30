from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("signup_date") >= "2023-03-09")
    .select(
        "id",
        "name",
        F.coalesce("credit_limit", F.lit(0)).alias("credit_limit_filled"),
    )
)
