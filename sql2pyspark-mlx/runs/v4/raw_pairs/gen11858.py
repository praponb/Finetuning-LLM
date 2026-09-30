from pyspark.sql import functions as F

result = (
    df_accounts
    .filter((F.col("balance") > 21000) & (F.col("balance") < 64000) & (F.col("account_type") == "LOAN"))
    .select("id", "opened_date")
)
