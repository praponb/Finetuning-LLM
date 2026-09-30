from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy(
        "txn_type",
        F.when(F.col("amount") >= 4000, "high").when(F.col("amount") >= 600, "mid").otherwise("low").alias("amount_bucket"),
    )
    .agg(
        F.sum(F.when(F.col("txn_type") == "DEBIT", F.col("amount")).otherwise(0)).alias("debit_amount"),
        F.round(((100 * F.sum(F.when(F.col("merchant") == "7-Eleven", 1).otherwise(0))) / F.count("*")), 1).alias("pct_7_eleven"),
    )
    .filter(F.col("debit_amount") > 21)
    .orderBy("txn_type")
)
