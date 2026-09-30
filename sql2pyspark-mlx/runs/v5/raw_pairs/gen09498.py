from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy("account_id", "txn_type")
    .pivot("merchant", ["Grab", "Shell"])
    .agg(F.avg("amount"))
    .select(
        "account_id",
        "txn_type",
        F.col("Grab").alias("grab"),
        F.col("Shell").alias("shell"),
    )
)
