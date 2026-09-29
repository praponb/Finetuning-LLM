from pyspark.sql import functions as F

result = (
    df_payments
    .filter((F.col("status") == "REFUNDED") & (F.to_date("paid_at") >= "2024-05-24"))
    .select("id", "method", "amount", "order_id")
)
