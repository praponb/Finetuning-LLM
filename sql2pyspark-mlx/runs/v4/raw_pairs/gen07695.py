from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("customer_id") == 12)
    .select("opened_date", F.lit("account").alias("source"))
    .union(df_customers.select("signup_date", F.lit("customer").alias("source")))
    .distinct()
)
