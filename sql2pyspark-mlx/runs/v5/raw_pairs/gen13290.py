from pyspark.sql import functions as F

cleaned = (
    df_transactions
    .filter(F.col("txn_time") >= F.lit("2023-08-21").cast("date"))
    .withColumn("amount_filled", F.coalesce("amount", F.lit(0)))
)

result = (
    cleaned
    .groupBy("account_id", "txn_type")
    .pivot("merchant", ["Amazon", "Shell", "Starbucks"])
    .agg(F.sum("amount").alias("total"))
    .select(
        "account_id",
        "txn_type",
        F.col("Amazon").alias("amazon"),
        F.col("Shell").alias("shell"),
        F.col("Starbucks").alias("starbucks"),
    )
)
