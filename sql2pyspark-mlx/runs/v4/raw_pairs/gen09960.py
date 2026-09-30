from pyspark.sql import functions as F

result = (
    df_accounts
    .groupBy("account_type")
    .agg(
        F.max("opened_date").alias("latest_opened_date"),
        F.round(F.stddev("balance"), 2).alias("stddev_balance"),
    )
    .filter(F.col("stddev_balance") > 34)
    .select("account_type", "latest_opened_date", "stddev_balance")
    .orderBy("latest_opened_date")
)
