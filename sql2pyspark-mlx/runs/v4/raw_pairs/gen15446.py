from pyspark.sql import functions as F

result = (
    df_transactions
    .filter((F.col("txn_time") < "2023-10-01") | F.hour("txn_time").between(6, 10))
    .groupBy(F.to_date("txn_time").alias("txn_day"))
    .agg(
        F.round(F.avg("amount"), 2).alias("avg_amount"),
        (F.max("amount") - F.min("amount")).alias("amount_range"),
    )
    .orderBy("txn_day")
)
