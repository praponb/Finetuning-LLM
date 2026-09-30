from pyspark.sql import functions as F

src = (
    df_transactions
    .filter((F.col("account_id") == 8) & (F.col("txn_type").isin(["REFUND", "FEE", "CREDIT"]) | F.col("account_id").isin([8, 34])))
)

agg = (
    src
    .groupBy("txn_type")
    .agg(
        F.max("txn_time").alias("last_txn_time"),
        F.concat_ws(", ", F.sort_array(F.collect_set("txn_type"))).alias("txn_type_values"),
    )
)

result = (
    agg
    .orderBy("txn_type", "last_txn_time")
    .limit(50)
)
