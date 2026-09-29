from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy(F.date_trunc("year", "txn_time").alias("year_start"))
    .agg(
        F.round(F.avg("amount"), 2).alias("avg_amount"),
        F.sort_array(F.collect_set("merchant")).alias("merchant_list"),
        F.count(F.when(F.col("txn_type") == "DEBIT", 1)).alias("debit_rows"),
        F.max("txn_time").alias("last_seen"),
    )
    .orderBy("year_start")
)
