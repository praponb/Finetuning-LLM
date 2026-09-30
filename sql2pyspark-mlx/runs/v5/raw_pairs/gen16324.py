from pyspark.sql import functions as F

valid = (
    df_transactions
    .filter(F.col("account_id").isin([16, 26, 35]))
    .select(
        "id",
        "account_id",
        "amount",
        "txn_time",
        "merchant",
        F.concat_ws("/", "txn_type", "merchant").alias("txn_type_merchant_key"),
    )
)

result = (
    valid
    .groupBy("account_id")
    .pivot("merchant", ["Amazon", "Starbucks"])
    .agg(F.sum("amount").alias("amount"), F.count(F.lit(1)).alias("cnt"))
    .select(
        "account_id",
        F.col("Amazon_amount").alias("amazon_amount"),
        F.col("Amazon_cnt").alias("amazon_cnt"),
        F.col("Starbucks_amount").alias("starbucks_amount"),
        F.col("Starbucks_cnt").alias("starbucks_cnt"),
    )
)
