from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.hour("paid_at").between(9, 16))
    .groupBy("method")
    .pivot("status", ["SUCCESS", "REFUNDED", "FAILED"])
    .agg(F.max("amount"))
)
