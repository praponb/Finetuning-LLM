from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("balance") >= 50000)
    .select("status", "account_type")
    .distinct()
)
