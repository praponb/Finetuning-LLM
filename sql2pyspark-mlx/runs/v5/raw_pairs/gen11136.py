from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("balance").isNotNull())
    .select("status")
    .intersect(df_subscriptions.select("status"))
)
