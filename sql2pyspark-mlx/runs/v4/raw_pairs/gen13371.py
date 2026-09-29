from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy("txn_type", F.hour("txn_time").alias("txn_hour"))
    .agg(
        F.sum("amount").alias("total_amount"),
        F.max("amount").alias("max_amount"),
    )
    .filter(F.col("max_amount") > 2400)
)
