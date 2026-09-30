from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("balance") >= 69000)
    .groupBy("account_type")
    .pivot("status", ["CLOSED", "FROZEN", "OPEN"])
    .agg(F.max("balance"))
)
