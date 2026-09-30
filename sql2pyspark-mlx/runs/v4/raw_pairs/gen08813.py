from pyspark.sql import functions as F

result = (
    df_payments
    .groupBy("status")
    .pivot("method", ["card", "promptpay", "bank_transfer"])
    .agg(F.max("amount"))
    .orderBy("status")
)
