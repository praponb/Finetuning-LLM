from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("balance") <= 25000)
    .select("customer_id")
    .unionByName(df_accounts.filter(F.col("status").isNull()).select("customer_id"))
)
