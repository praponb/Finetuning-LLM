from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("balance") > 61000)
    .select("status")
    .subtract(df_subscriptions.select("status"))
)
