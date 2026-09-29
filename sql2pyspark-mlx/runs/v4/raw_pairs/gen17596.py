from pyspark.sql import functions as F

result = (
    df_accounts
    .groupBy("account_type")
    .pivot("status", ["CLOSED", "FROZEN"])
    .agg(F.max("balance"))
    .select(
        "account_type",
        F.col("CLOSED").alias("closed"),
        F.col("FROZEN").alias("frozen"),
    )
)
