from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("customer_id") == 6)
    .select("account_type")
    .distinct()
)
