from pyspark.sql import functions as F

result = (
    df_orders
    .select("channel")
    .distinct()
    .crossJoin(df_accounts.select("account_type").distinct())
    .orderBy("channel", "account_type")
)
