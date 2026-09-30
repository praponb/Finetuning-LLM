from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(~F.col("status").isin(["FROZEN", "OPEN"]))
    .groupBy("status")
    .agg(
        F.max("balance").alias("max_balance"),
        F.sum(F.when(F.col("status") == "OPEN", 1).otherwise(0)).alias("open_count"),
    )
)
