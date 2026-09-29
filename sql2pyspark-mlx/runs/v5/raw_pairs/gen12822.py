from pyspark.sql import functions as F

result = (
    df_accounts
    .select("account_type")
    .distinct()
    .crossJoin(df_subscriptions.select("plan").distinct())
    .orderBy("account_type", "plan")
)
