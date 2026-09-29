from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("status") != "FROZEN")
    .select("customer_id")
    .subtract(df_accounts.filter(F.col("balance") > 61000).select("customer_id"))
)
