from pyspark.sql import functions as F

result = (
    df_accounts
    .filter((F.col("account_type") == "SAVINGS") & (F.col("balance") >= 72000) & (F.col("balance") > 77000))
    .select("balance", "opened_date")
)
