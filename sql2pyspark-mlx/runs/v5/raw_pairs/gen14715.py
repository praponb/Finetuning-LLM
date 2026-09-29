from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy(F.hour("txn_time").alias("txn_hour"))
    .agg(
        F.sum(F.when(F.col("merchant") == "Shell", 1).otherwise(0)).alias("shell_count"),
        F.min("amount").alias("min_amount"),
    )
    .filter(F.col("min_amount") > 2700)
)
