from pyspark.sql import functions as F

result = (
    df_payments
    .filter((F.col("paid_at") < "2023-05-01") & F.col("amount").isNull())
    .select("order_id", "status", "method", F.length("method").alias("method_length"))
)
