from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("account_id").isin([3, 6, 22, 38]) & (F.col("account_id") == 35))
    .groupBy(F.month("txn_time").alias("txn_month"))
    .agg(
        F.min("amount").alias("min_amount"),
        F.max("amount").alias("max_amount"),
        F.sort_array(F.collect_set("merchant")).alias("merchant_list"),
    )
)
