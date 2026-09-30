from pyspark.sql import functions as F

result = (
    df_payments
    .groupBy("order_id", "method")
    .pivot("status", ["SUCCESS", "FAILED", "REFUNDED"])
    .agg(F.max("amount"))
)
