from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("status") != "FROZEN")
    .groupBy("status")
    .agg(
        F.round(F.stddev("balance"), 2).alias("stddev_balance"),
        F.sum(F.when(F.col("status") == "CLOSED", F.col("balance")).otherwise(0)).alias("closed_balance"),
    )
    .filter(F.col("stddev_balance") >= 72)
    .select("status", "stddev_balance", "closed_balance")
    .orderBy(F.col("status").desc())
)
