from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(((F.col("amount") >= 3800) & (F.col("txn_type") != "REFUND")) | (F.col("amount") < 1800))
    .groupBy("account_id")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("txn_type"))).alias("txn_type_values"),
        F.min("txn_time").alias("min_txn_time"),
    )
    .orderBy(F.col("min_txn_time").desc(), "account_id")
    .limit(10)
)
