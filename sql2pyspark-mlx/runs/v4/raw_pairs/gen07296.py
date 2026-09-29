from pyspark.sql import functions as F

result = (
    df_accounts
    .select("opened_date")
    .union(df_customers.filter(F.col("is_active")).select("signup_date"))
)
