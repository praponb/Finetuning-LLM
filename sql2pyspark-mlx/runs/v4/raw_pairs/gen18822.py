from pyspark.sql import functions as F

result = (
    df_payments
    .groupBy("order_id", "method")
    .pivot("status", ["REFUNDED", "FAILED"])
    .agg(F.avg("amount").alias("amount"), F.count(F.lit(1)).alias("cnt"))
    .select(
        "order_id",
        "method",
        F.col("REFUNDED_amount").alias("refunded_amount"),
        F.col("REFUNDED_cnt").alias("refunded_cnt"),
        F.col("FAILED_amount").alias("failed_amount"),
        F.col("FAILED_cnt").alias("failed_cnt"),
    )
)
