from pyspark.sql import functions as F

filtered_transactions = df_transactions.filter(F.col("amount") <= 2500)

txn_year_summary = (
    filtered_transactions
    .groupBy(F.year("txn_time").alias("txn_year"))
    .agg(
        F.count(F.when(F.col("txn_type") == "CREDIT", 1)).alias("credit_count"),
        F.max("txn_time").alias("latest_txn_time"),
    )
)

result = (
    txn_year_summary
    .filter(F.col("credit_count") >= 4)
    .select("txn_year", "latest_txn_time")
    .orderBy("latest_txn_time", F.col("txn_year").desc())
)
