from pyspark.sql import functions as F

result = (
    df_accounts
    .select("opened_date")
    .unionByName(df_subscriptions.select(F.col("start_date").alias("opened_date")))
    .distinct()
    .orderBy(F.col("opened_date").desc())
)
