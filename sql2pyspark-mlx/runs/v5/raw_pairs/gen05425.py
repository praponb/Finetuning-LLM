from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.hour("paid_at").between(6, 12))
    .groupBy("method")
    .pivot("status", ["REFUNDED", "FAILED", "SUCCESS"])
    .agg(F.count("amount"))
    .orderBy("method")
)
