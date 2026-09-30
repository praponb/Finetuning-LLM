from pyspark.sql import functions as F

result = (
    df_payments
    .groupBy("status")
    .pivot("method", ["paypal", "cash", "card"])
    .agg(F.avg("amount"))
    .orderBy("status")
)
