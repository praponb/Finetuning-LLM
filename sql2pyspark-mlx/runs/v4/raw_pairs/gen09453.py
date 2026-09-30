from pyspark.sql import functions as F

result = (
    df_payments
    .filter((F.hour("paid_at").between(10, 15) & (F.col("status") == "SUCCESS")) | F.col("order_id").isNull())
    .groupBy("status", "order_id")
    .agg(F.round(F.avg("amount"), 2).alias("avg_amount"))
)
