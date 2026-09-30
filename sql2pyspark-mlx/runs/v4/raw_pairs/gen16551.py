from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("order_id") == 26)
    .select("status", "paid_at", "amount")
)
