from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("account_id").isin([6, 13, 36, 40]))
    .groupBy(
        F.when(F.col("merchant").isin(["Lazada", "Amazon"]), "priority").otherwise("other").alias("merchant_group"),
    )
    .agg(
        F.max("amount").alias("max_amount"),
        F.sort_array(F.collect_set("txn_type")).alias("txn_type_list"),
    )
)
