from pyspark.sql import functions as F

result = (
    df_payments
    .groupBy("status")
    .pivot("method", ["card", "cash", "promptpay", "bank_transfer"])
    .agg(F.max("amount"))
)
