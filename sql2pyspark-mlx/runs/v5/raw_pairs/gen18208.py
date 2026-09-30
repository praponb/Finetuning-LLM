from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy(F.year("txn_time").alias("txn_year"))
    .agg(
        F.max("txn_time").alias("latest_txn_time"),
        F.max("amount").alias("max_amount"),
    )
    .orderBy(F.col("latest_txn_time").desc(), "txn_year")
    .limit(50)
)
