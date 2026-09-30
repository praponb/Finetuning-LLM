from pyspark.sql import functions as F

result = (
    df_transactions
    .filter((F.col("txn_type") == "REFUND") & (F.col("txn_type") == "CREDIT"))
    .groupBy("txn_type")
    .agg(
        F.countDistinct("amount").alias("unique_merchants"),
        F.count(F.when(F.col("merchant") == "7-Eleven", 1)).alias("7_eleven_count"),
    )
    .filter(F.col("unique_merchants") >= 4)
    .orderBy("txn_type")
)
