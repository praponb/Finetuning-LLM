from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("customer_id").isNotNull())
    .select("status")
    .unionByName(df_subscriptions.select("status"))
)
