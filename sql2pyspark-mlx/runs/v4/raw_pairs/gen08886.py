from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy(
        F.month("txn_time").alias("txn_month"),
        F.when(F.col("amount") >= 3500, "high").when(F.col("amount") >= 2000, "mid").otherwise("low").alias("amount_bucket"),
    )
    .agg(F.count(F.when(F.col("merchant") == "7-Eleven", 1)).alias("7_eleven_count"))
    .filter(F.col("7_eleven_count") < 6)
    .select("txn_month", "amount_bucket", "7_eleven_count")
    .orderBy(F.col("amount_bucket").desc())
)
