from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("merchant") == "Grab")
    .groupBy(F.quarter("txn_time").alias("txn_quarter"))
    .agg(
        F.round(F.avg("amount"), 2).alias("avg_amount"),
        F.count("amount").alias("amount_count"),
    )
    .filter(F.col("avg_amount") > 60)
)
