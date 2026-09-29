from pyspark.sql import functions as F

result = (
    df_payments
    .filter(~((F.col("paid_at") >= "2024-11-07") & (F.col("order_id") == 37)))
    .select("id", "method", "paid_at")
)
