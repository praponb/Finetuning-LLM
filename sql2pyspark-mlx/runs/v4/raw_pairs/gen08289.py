from pyspark.sql import functions as F

result = (
    df_payments
    .groupBy("order_id")
    .pivot("method", ["promptpay", "card"])
    .agg(F.sum("amount"))
)
