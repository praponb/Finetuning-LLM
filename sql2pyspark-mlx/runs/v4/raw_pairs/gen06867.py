from pyspark.sql import functions as F

result = (
    df_accounts
    .groupBy("customer_id")
    .agg(
        F.sum(F.when(F.col("status") == "CLOSED", 1).otherwise(0)).alias("closed_count"),
        F.count("balance").alias("balance_count"),
        F.max("balance").alias("max_balance"),
    )
    .orderBy(F.col("closed_count").desc())
)
