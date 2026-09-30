from pyspark.sql import functions as F

result = (
    df_payments
    .groupBy("status")
    .agg(F.max("paid_at").alias("latest_paid_at"))
    .orderBy("latest_paid_at", "status")
)
