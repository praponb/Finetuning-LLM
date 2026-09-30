from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy("is_flagged", F.month("txn_time").alias("txn_month"))
    .agg(
        F.sort_array(F.collect_set("merchant")).alias("merchant_list"),
        F.concat_ws(", ", F.sort_array(F.collect_set("txn_type"))).alias("txn_type_values"),
    )
    .orderBy("txn_type_values", "txn_month")
)
