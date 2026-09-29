from pyspark.sql import functions as F

valid_transactions = df_transactions.filter(F.col("account_id").isin([9, 24, 38]))

txn_month_summary = (
    valid_transactions
    .groupBy(F.month("txn_time").alias("txn_month"))
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("merchant"))).alias("merchant_values"),
    )
)

result = txn_month_summary.orderBy("merchant_values")
