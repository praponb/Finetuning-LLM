from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy("txn_type")
    .agg(
        F.sort_array(F.collect_set("txn_type")).alias("txn_type_list"),
        F.round(((100 * F.sum(F.when(F.col("txn_type") == "REFUND", 1).otherwise(0))) / F.count("*")), 1).alias("pct_refund"),
    )
    .filter(F.col("pct_refund") < 63)
)
