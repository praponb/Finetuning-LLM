from pyspark.sql import functions as F

result = (
    df_transactions
    .filter((F.col("txn_type").isin(["FEE", "DEBIT", "CREDIT"]) & F.col("txn_type").isin(["CREDIT", "REFUND"])) | F.col("is_flagged"))
    .groupBy("merchant")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("txn_type"))).alias("txn_type_values"),
    )
)
