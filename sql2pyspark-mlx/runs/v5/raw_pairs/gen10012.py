from pyspark.sql import functions as F

result = (
    df_payments
    .filter((F.col("amount") <= 1540) & ((F.col("method") == "paypal") | (F.col("paid_at") < "2023-04-01")))
    .select("paid_at", "order_id")
)
