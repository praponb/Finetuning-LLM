from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("balance") > 50000)
    .groupBy("account_type")
    .agg(
        F.avg("balance").alias("avg_balance"),
        F.round(F.stddev("balance"), 2).alias("stddev_balance"),
        F.count("*").alias("cnt"),
    )
)
