from pyspark.sql import functions as F

result = (
    df_transactions
    .agg(
        F.round(((100 * F.sum(F.when(F.col("txn_type") == "DEBIT", 1).otherwise(0))) / F.count("*")), 1).alias("pct_debit"),
        F.count(F.when(F.col("merchant") == "7-Eleven", 1)).alias("7_eleven_count"),
        F.max("amount").alias("max_amount"),
    )
    .orderBy(F.col("7_eleven_count").desc())
)
