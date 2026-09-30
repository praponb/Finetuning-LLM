from pyspark.sql import functions as F

result = (
    df_payments
    .filter(~(F.col("order_id").isNotNull() & (F.col("method") != "card")))
    .select("paid_at")
)
