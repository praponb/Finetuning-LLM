from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("account_type").isin(["LOAN", "SAVINGS"]) & (F.col("account_type") == "SAVINGS") & (F.col("balance") < 70000))
    .orderBy("account_type")
)
