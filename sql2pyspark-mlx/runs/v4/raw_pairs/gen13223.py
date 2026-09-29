from pyspark.sql import functions as F

result = (
    df_transactions
    .filter((F.col("txn_type") == "CREDIT") & (F.col("txn_type") == "CREDIT") & F.col("merchant").isin(["Lazada", "Starbucks"]))
    .groupBy("txn_type")
    .agg(
        F.sum(F.when(F.col("txn_type") == "DEBIT", F.col("amount")).otherwise(0)).alias("debit_amount"),
        F.round(F.stddev("amount"), 2).alias("stddev_amount"),
        F.concat_ws(", ", F.sort_array(F.collect_set("txn_type"))).alias("txn_type_values"),
    )
)
