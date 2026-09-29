from pyspark.sql import functions as F

result = (
    df_accounts
    .groupBy(F.quarter("opened_date").alias("opened_quarter"))
    .agg(
        F.avg("balance").alias("avg_balance"),
        F.count("balance").alias("balance_count"),
        F.max("opened_date").alias("latest_opened_date"),
    )
)
