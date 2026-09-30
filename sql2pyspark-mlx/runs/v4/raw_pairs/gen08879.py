from pyspark.sql import functions as F

result = (
    df_accounts
    .groupBy(F.date_trunc("MONTH", "opened_date").alias("opened_month"))
    .agg(
        F.count("*").alias("total_count"),
        F.sum(F.when(F.col("status") == "CLOSED", F.col("balance")).otherwise(0)).alias("closed_balance"),
        F.round(F.stddev("balance"), 2).alias("stddev_balance"),
    )
    .orderBy("stddev_balance")
)
