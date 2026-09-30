from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("merchant").isin(["Amazon", "Shell"]) & (F.col("amount") > 2800))
    .groupBy("is_flagged", F.to_date("txn_time").alias("txn_day"))
    .agg(
        F.avg("amount").alias("avg_amount"),
        F.round(F.stddev("amount"), 2).alias("stddev_amount"),
    )
    .filter(F.col("avg_amount") < 3300)
)
