from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("is_flagged"))
    .groupBy("txn_type")
    .agg(
        F.sum(F.when(F.col("txn_type") == "REFUND", F.col("amount")).otherwise(0)).alias("refund_amount"),
        F.concat_ws(", ", F.sort_array(F.collect_set("merchant"))).alias("merchant_values"),
    )
    .filter(F.col("refund_amount") >= 76)
    .orderBy(F.col("txn_type").desc(), F.col("merchant_values").desc())
)
