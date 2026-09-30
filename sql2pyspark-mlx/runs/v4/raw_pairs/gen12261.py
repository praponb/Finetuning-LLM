from pyspark.sql import functions as F

result = (
    df_accounts
    .select("id", "customer_id", "account_type", "status")
    .orderBy(F.col("account_type").desc())
)
