from pyspark.sql import functions as F

result = (
    df_payments
    .filter((F.to_date("paid_at") >= "2023-04-01") & (F.col("order_id").isNotNull() | F.col("amount").between(760, 980)))
    .select("order_id", "status", "amount")
    .orderBy("order_id")
)
