from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(~F.col("account_type").isin(["LOAN", "CREDIT", "SAVINGS"]))
    .select("status")
    .unionByName(df_subscriptions.select("status"))
    .distinct()
)
