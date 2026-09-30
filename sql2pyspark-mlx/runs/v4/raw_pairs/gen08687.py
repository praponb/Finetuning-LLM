from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("merchant").isin(["Amazon", "Lazada"]))
    .groupBy("merchant")
    .agg(
        F.sum(F.when(F.col("txn_type") == "REFUND", 1).otherwise(0)).alias("refund_count"),
        F.sort_array(F.collect_set("txn_type")).alias("txn_type_list"),
    )
    .filter(F.col("refund_count") >= 54)
    .select("merchant", "refund_count", "txn_type_list")
    .orderBy(F.col("refund_count").desc())
)
