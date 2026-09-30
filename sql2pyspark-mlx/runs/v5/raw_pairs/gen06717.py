from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy(
        "is_flagged",
        "merchant",
        F.when(F.col("txn_type").isin(["REFUND", "DEBIT"]), "group_a").otherwise("other").alias("txn_type_group"),
    )
    .agg(
        F.sum(F.when(F.col("txn_type") == "FEE", 1).otherwise(0)).alias("fee_count"),
        F.round(((100 * F.sum(F.when(F.col("merchant") == "Starbucks", 1).otherwise(0))) / F.count("*")), 1).alias("pct_starbucks"),
        F.count("*").alias("cnt"),
    )
    .filter(F.col("cnt") > 2)
)
