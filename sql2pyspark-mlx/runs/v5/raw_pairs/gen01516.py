from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("amount").between(3400, 3600) & (F.col("txn_type").isin(["DEBIT", "FEE", "REFUND"]) | (F.col("amount") < 2400)))
    .groupBy("merchant")
    .agg(
        F.sum("amount").alias("total_amount"),
        F.concat_ws(", ", F.sort_array(F.collect_set("merchant"))).alias("merchant_values"),
    )
)
