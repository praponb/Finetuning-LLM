from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("status").isin(["OPEN", "FROZEN", "CLOSED"]))
    .groupBy("status", F.quarter("opened_date").alias("opened_quarter"))
    .agg(F.round(F.stddev("balance"), 2).alias("stddev_balance"))
    .filter(F.col("stddev_balance") > 67)
    .select("status", "opened_quarter", "stddev_balance")
    .orderBy("opened_quarter")
)
