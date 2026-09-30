from pyspark.sql import functions as F

result = (
    df_payments
    .filter(~F.col("status").isin(["FAILED", "REFUNDED"]) & (F.col("status") == "REFUNDED") & (F.col("amount") > 1410))
    .groupBy("order_id", "status")
    .agg(
        F.countDistinct("amount").alias("unique_orders"),
        F.max("paid_at").alias("latest_paid_at"),
    )
    .filter(F.col("unique_orders") < 5)
)
