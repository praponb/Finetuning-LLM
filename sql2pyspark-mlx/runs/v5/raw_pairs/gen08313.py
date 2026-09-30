from pyspark.sql import functions as F

result = (
    df_payments
    .filter((F.col("status") == "REFUNDED") & (F.col("status") == "SUCCESS") & (F.col("method") == "paypal"))
    .orderBy(F.col("order_id").desc(), "id")
    .limit(5)
)
