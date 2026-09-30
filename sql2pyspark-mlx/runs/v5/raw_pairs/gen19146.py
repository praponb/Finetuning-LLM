from pyspark.sql import functions as F

result = (
    df_payments
    .filter(~F.col("method").isin(["card", "paypal"]) & (F.col("paid_at") < "2023-09-01"))
    .groupBy("method")
    .agg(
        F.sum(F.when(F.col("method") == "cash", 1).otherwise(0)).alias("cash_count"),
        F.max("amount").alias("max_amount"),
    )
    .filter(F.col("max_amount") >= 630)
    .drop("max_amount")
    .orderBy("cash_count")
)
