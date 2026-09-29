from pyspark.sql import functions as F

result = (
    df_accounts
    .select("opened_date", F.lit("account").alias("source"))
    .union(
        df_customers
        .filter(F.col("is_active"))
        .select("signup_date", F.lit("customer").alias("source"))
    )
)
