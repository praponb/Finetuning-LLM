from pyspark.sql import functions as F

result = (
    df_accounts
    .groupBy("account_type")
    .agg(
        F.round(F.avg("balance"), 2).alias("avg_balance"),
        F.round(F.stddev("balance"), 2).alias("stddev_balance"),
    )
)
