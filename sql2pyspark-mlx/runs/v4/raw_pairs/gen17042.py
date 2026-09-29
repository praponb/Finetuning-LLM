from pyspark.sql import functions as F

result = (
    df_accounts
    .select("opened_date")
    .subtract(df_customers.select(F.col("signup_date").alias("opened_date")))
    .orderBy("opened_date")
)
