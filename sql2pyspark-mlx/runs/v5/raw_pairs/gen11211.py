from pyspark.sql import functions as F

result = (
    df_accounts
    .select("opened_date", F.lit("account").alias("origin"))
    .union(df_customers.select("signup_date", F.lit("customer").alias("origin")))
    .distinct()
)
