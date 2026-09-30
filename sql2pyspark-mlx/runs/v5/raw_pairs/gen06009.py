from pyspark.sql import functions as F

result = (
    df_payments
    .filter((F.col("paid_at") < "2023-06-22") & F.col("method").isNotNull())
    .select("id", "order_id", F.substring("method", 1, 3).alias("method_prefix"))
)
