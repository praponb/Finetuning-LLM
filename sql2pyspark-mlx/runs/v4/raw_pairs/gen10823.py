from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy(F.quarter("txn_time").alias("txn_quarter"), "is_flagged")
    .agg(
        F.min("txn_time").alias("min_txn_time"),
        F.concat_ws(", ", F.sort_array(F.collect_set("txn_type"))).alias("txn_type_values"),
    )
    .orderBy(F.col("txn_quarter").desc())
)
