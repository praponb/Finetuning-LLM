from pyspark.sql import functions as F

recent_payments = (
    df_payments
    .filter(F.col("method").isin(["card", "bank_transfer", "cash"]) | F.col("status").isin(["SUCCESS", "REFUNDED", "FAILED"]))
    .withColumn("paid_at_hour", F.hour("paid_at"))
)

method_summary = (
    recent_payments
    .groupBy("method")
    .agg(F.min("paid_at").alias("first_paid_at"))
)

result = (
    method_summary
    .select("method", "first_paid_at")
    .orderBy(F.col("method").desc())
)
