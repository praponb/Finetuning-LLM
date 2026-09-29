from pyspark.sql import functions as F

result = (
    df_customers
    .select("signup_date")
    .subtract(df_accounts.select(F.col("opened_date").alias("signup_date")))
)
