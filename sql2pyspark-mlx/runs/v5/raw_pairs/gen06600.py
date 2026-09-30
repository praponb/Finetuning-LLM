from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy("account_id", F.date_trunc("MONTH", "txn_time").alias("txn_month"))
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("merchant"))).alias("merchant_values"),
        F.sort_array(F.collect_set("txn_type")).alias("txn_type_list"),
        F.min("amount").alias("min_amount"),
    )
    .filter(F.col("min_amount") < 3200)
    .orderBy("merchant_values", "account_id", "txn_month")
    .limit(5)
)
