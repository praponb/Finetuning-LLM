from pyspark.sql import functions as F

min_amount = df_payments.filter(F.col("paid_at") < "2025-03-01").agg(F.min("amount")).first()[0]

result = (
    df_payments
    .filter(F.col("amount") == min_amount)
    .select("id", "amount", "method", "order_id")
    .orderBy(F.col("order_id").desc(), "id")
    .limit(1)
)
