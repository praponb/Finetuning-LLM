from pyspark.sql import functions as F

valid_transactions = (
    df_transactions
    .filter(F.col("account_id").isin([3, 34]) | F.col("merchant").isin(["7-Eleven", "Amazon"]))
)

agg = (
    valid_transactions
    .groupBy(
        F.when(F.year("txn_time") < 2024, "before_2024").when(F.year("txn_time") == 2024, "2024").otherwise("after_2024").alias("txn_cohort"),
        "txn_type",
    )
    .agg(
        F.sort_array(F.collect_set("txn_type")).alias("txn_type_list"),
        F.concat_ws(", ", F.sort_array(F.collect_set("merchant"))).alias("merchant_values"),
    )
)

result = (
    agg
    .orderBy("merchant_values", "txn_type", "txn_cohort")
    .limit(5)
)
