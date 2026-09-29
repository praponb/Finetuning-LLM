from pyspark.sql import functions as F

result = (
    df_transactions
    .agg(
        F.sum("amount").alias("total_amount"),
        F.count(F.when(F.col("merchant") == "7-Eleven", 1)).alias("7_eleven_count"),
        F.avg("amount").alias("avg_amount"),
    )
    .orderBy(F.col("total_amount").desc(), "avg_amount")
)
