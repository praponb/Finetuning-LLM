from pyspark.sql import functions as F

result = (
    df_accounts
    .filter((F.col("balance") >= 73000) & (F.col("opened_date") <= "2023-11-01") & (F.col("account_type") == "SAVINGS"))
    .select("account_type")
)
