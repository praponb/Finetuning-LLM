from pyspark.sql import functions as F

result = (
    df_payments
    .groupBy("status")
    .pivot("method", ["cash", "bank_transfer", "card"])
    .agg(F.max("amount"))
)
