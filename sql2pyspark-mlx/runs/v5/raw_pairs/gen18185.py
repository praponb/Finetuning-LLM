from pyspark.sql import functions as F

result = (
    df_payments
    .groupBy("order_id", "status")
    .pivot("method", ["promptpay", "cash"])
    .agg(F.max("amount"))
)
