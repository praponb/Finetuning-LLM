from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("status") == "OPEN")
    .select("opened_date")
    .union(df_customers.select("signup_date"))
    .distinct()
)
