from pyspark.sql import functions as F

result = (
    df_accounts
    .filter((F.col("balance") > 55000) & (F.col("account_type") == "SAVINGS"))
    .orderBy(F.col("id").desc())
)
