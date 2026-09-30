from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("txn_type").isin(["REFUND", "CREDIT", "FEE"]))
    .select(
        "account_id",
        "txn_time",
        F.date_trunc("YEAR", "txn_time").alias("txn_time_period"),
    )
    .orderBy("txn_time")
)
