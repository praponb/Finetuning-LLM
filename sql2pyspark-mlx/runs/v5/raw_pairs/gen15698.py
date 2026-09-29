from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy(
        F.hour("txn_time").alias("txn_hour"),
        F.when(F.col("amount") >= 4000, "high").when(F.col("amount") >= 700, "mid").otherwise("low").alias("amount_bucket"),
    )
    .agg(
        F.avg("amount").alias("avg_amount"),
        F.round(F.stddev("amount"), 2).alias("stddev_amount"),
    )
    .filter(F.col("avg_amount") > 2000)
    .orderBy(F.col("txn_hour").desc(), "amount_bucket")
    .limit(50)
)
