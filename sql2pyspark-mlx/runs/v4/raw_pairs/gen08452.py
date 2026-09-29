from pyspark.sql import functions as F

result = (
    df_transactions
    .select(
        "id",
        "amount",
        "merchant",
        F.dayofweek("txn_time").alias("txn_time_dayofweek"),
        F.substring("merchant", 1, 2).alias("merchant_prefix"),
    )
    .orderBy("merchant_prefix", "merchant")
)
