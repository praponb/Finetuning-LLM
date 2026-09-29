from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy("txn_type", F.quarter("txn_time").alias("txn_quarter"))
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("txn_type"))).alias("txn_type_values"),
    )
    .orderBy("txn_type_values")
)
