from pyspark.sql import functions as F

filtered_transactions = (
    df_transactions
    .filter(F.col("amount").isNull())
    .withColumn("is_debit", F.when(F.col("txn_type") == "DEBIT", 1).otherwise(0))
)

summary = (
    filtered_transactions
    .groupBy("is_flagged")
    .agg(
        F.sum(F.when(F.col("merchant") == "Amazon", F.col("is_debit")).otherwise(0)).alias("amazon_is_debit"),
    )
)

result = (
    summary
    .filter(F.col("amazon_is_debit") >= 77)
    .select("is_flagged", "amazon_is_debit")
    .orderBy(F.col("amazon_is_debit").desc())
)
