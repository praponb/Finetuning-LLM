from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy(F.date_trunc("MONTH", "txn_time").alias("txn_month"))
    .agg(
        F.round(((100 * F.sum(F.when(F.col("merchant") == "Lazada", 1).otherwise(0))) / F.count("*")), 1).alias("pct_lazada"),
    )
    .filter(F.col("pct_lazada") >= 40)
    .select("txn_month", "pct_lazada")
    .orderBy("txn_month")
)
