from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("order_id").isNull())
    .select("id", "paid_at", "status", "order_id")
)
