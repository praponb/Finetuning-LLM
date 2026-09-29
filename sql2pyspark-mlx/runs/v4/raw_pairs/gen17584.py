from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy(F.date_trunc("MONTH", "txn_time").alias("txn_month"))
    .agg(
        F.countDistinct("amount").alias("unique_accounts"),
        F.concat_ws(", ", F.sort_array(F.collect_set("merchant"))).alias("merchant_values"),
    )
)
