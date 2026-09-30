from pyspark.sql import functions as F

result = (
    df_accounts
    .groupBy("status")
    .agg(
        F.round(F.stddev("balance"), 2).alias("stddev_balance"),
        F.min("balance").alias("min_balance"),
        F.avg("balance").alias("avg_balance"),
    )
    .orderBy(F.col("min_balance").desc())
)
