from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy("txn_type", F.quarter("txn_time").alias("txn_quarter"), "account_id")
    .agg(
        F.sort_array(F.collect_set("txn_type")).alias("txn_type_list"),
        F.round(F.stddev("amount"), 2).alias("stddev_amount"),
    )
)
