from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("order_id").isin([4, 14, 39]) | (F.col("paid_at") >= "2023-07-01"))
    .withColumn("paid_at_hour", F.hour("paid_at"))
    .withColumn("method_filled", F.coalesce("method", F.lit("Unknown")))
    .orderBy("amount")
)
