from pyspark.sql import functions as F

valid_transactions = (
    df_transactions
    .filter((F.col("account_id") == 4) & ((F.col("account_id") == 30) | F.col("is_flagged")))
    .withColumn("merchant_txn_type_key", F.concat_ws("/", "merchant", "txn_type"))
)

agg = (
    valid_transactions
    .groupBy(
        F.when(F.col("amount") >= 4000, "high").when(F.col("amount") >= 1700, "mid").otherwise("low").alias("amount_bucket"),
    )
    .agg(
        F.max("txn_time").alias("latest_txn_time"),
        F.sort_array(F.collect_set("merchant")).alias("merchant_list"),
    )
)

result = (
    agg
    .orderBy("amount_bucket")
    .limit(50)
)
