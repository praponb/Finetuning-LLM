from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy(F.date_trunc("MONTH", "txn_time").alias("txn_month"), "txn_type")
    .agg(
        F.count("*").alias("num_transactions"),
        (F.max("amount") - F.min("amount")).alias("amount_range"),
        F.max("txn_time").alias("latest_txn_time"),
    )
    .orderBy("amount_range", "latest_txn_time", "txn_month", "txn_type")
    .limit(50)
)
