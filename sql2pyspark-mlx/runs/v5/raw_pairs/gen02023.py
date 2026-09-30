from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("merchant").isin(["7-Eleven", "Starbucks", "Lazada"]))
    .groupBy(
        F.when(F.col("amount") >= 4500, "high").when(F.col("amount") >= 1300, "mid").otherwise("low").alias("amount_bucket"),
        "txn_type",
    )
    .agg(
        F.sum("amount").alias("total_amount"),
        F.countDistinct("amount").alias("unique_txn_types"),
        F.round(((100 * F.sum(F.when(F.col("merchant") == "7-Eleven", 1).otherwise(0))) / F.count("*")), 1).alias("pct_7_eleven"),
    )
    .orderBy("txn_type", "amount_bucket")
    .limit(1)
)
