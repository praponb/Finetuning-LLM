from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("order_id").isNull() & (F.to_date("paid_at") < "2023-04-20") & (F.col("order_id") == 1))
    .groupBy("status")
    .agg(F.min("paid_at").alias("min_paid_at"))
    .orderBy("status")
)
