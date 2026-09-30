from pyspark.sql import functions as F

base = df_transactions.filter(~F.col("is_flagged") | (F.col("amount") >= 2500))

merchant_summary = (
    base
    .groupBy("merchant")
    .agg(F.count(F.when(F.col("txn_type") == "REFUND", 1)).alias("refund_count"))
)

result = (
    merchant_summary
    .filter(F.col("refund_count") < 9)
    .select("merchant", "refund_count")
    .orderBy(F.col("merchant").desc())
)
