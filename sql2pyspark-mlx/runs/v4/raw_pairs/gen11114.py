from pyspark.sql import functions as F

result = (
    df_transactions
    .filter((F.col("is_flagged") & F.col("txn_type").isin(["FEE", "CREDIT", "REFUND"])) | F.col("account_id").isin([10, 23, 31]))
    .groupBy(
        F.date_trunc("MONTH", "txn_time").alias("txn_month"),
        F.to_date("txn_time").alias("txn_day"),
    )
    .agg(F.avg("amount").alias("avg_amount"))
    .orderBy("txn_day")
)
