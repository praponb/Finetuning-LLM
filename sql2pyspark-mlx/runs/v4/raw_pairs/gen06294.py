from pyspark.sql import functions as F

base = (
    df_transactions
    .filter((F.col("txn_type") == "FEE") & F.col("txn_type").isin(["REFUND", "FEE", "DEBIT"]))
    .withColumn("amount_rounded", F.round("amount", 2))
)

transactions_stats = (
    base
    .groupBy("is_flagged")
    .agg(
        F.countDistinct("amount_rounded").alias("unique_accounts"),
        F.concat_ws(", ", F.sort_array(F.collect_set("merchant"))).alias("merchant_values"),
    )
)

result = (
    transactions_stats
    .select("is_flagged", "merchant_values")
    .orderBy("merchant_values", "is_flagged")
    .limit(3)
)
