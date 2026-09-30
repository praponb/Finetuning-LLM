from pyspark.sql import functions as F

result = (
    df_accounts
    .groupBy("account_type")
    .pivot("status", ["FROZEN", "CLOSED"])
    .agg(F.sum("balance"))
    .select(
        "account_type",
        F.col("FROZEN").alias("frozen"),
        F.col("CLOSED").alias("closed"),
    )
)
