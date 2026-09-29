from pyspark.sql import functions as F

result = (
    df_payments
    .groupBy(
        F.quarter("paid_at").alias("paid_quarter"),
        F.to_date("paid_at").alias("paid_day"),
    )
    .agg(
        F.count("*").alias("row_count"),
        F.sum(F.when(F.col("method") == "cash", F.col("amount")).otherwise(0)).alias("cash_amount"),
        F.min("amount").alias("min_amount"),
        F.count(F.when(F.col("method") == "promptpay", 1)).alias("promptpay_count"),
    )
    .filter(F.col("promptpay_count") < 1)
    .drop("promptpay_count")
)
