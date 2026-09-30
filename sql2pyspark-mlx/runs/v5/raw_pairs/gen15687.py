from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("is_flagged") & (F.col("txn_type") != "CREDIT") & ~F.col("is_flagged"))
    .groupBy(
        F.when(F.col("merchant").isin(["7-Eleven", "Lazada"]), "core").otherwise("other").alias("merchant_group"),
        F.date_trunc("MONTH", "txn_time").alias("txn_month"),
    )
    .agg(
        F.round(((100 * F.sum(F.when(F.col("txn_type") == "CREDIT", 1).otherwise(0))) / F.count("*")), 1).alias("pct_credit"),
    )
)
