from pyspark.sql import functions as F

src = (
    df_transactions
    .filter(F.col("account_id").isin([11, 12, 13, 37]) & (F.col("txn_type") == "FEE") & F.col("amount").between(3600, 3800))
    .withColumn("merchant_lower", F.lower("merchant"))
)

agg = (
    src
    .groupBy("txn_type")
    .agg(
        F.count("amount").alias("amount_count"),
        F.sum(F.when(F.col("merchant") == "7-Eleven", F.col("amount")).otherwise(0)).alias("7_eleven_amount"),
    )
)

result = (
    agg
    .filter(F.col("amount_count") > 8)
    .select("txn_type", "amount_count", "7_eleven_amount")
)
