from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("opened_date") > "2023-11-17")
    .select("status")
    .unionByName(df_subscriptions.select("status"))
    .distinct()
    .orderBy("status")
)
