from pyspark.sql import functions as F

result = (
    df_accounts
    .groupBy("status")
    .agg(
        F.count("*").alias("cnt"),
        F.avg("balance").alias("avg_balance"),
    )
)
