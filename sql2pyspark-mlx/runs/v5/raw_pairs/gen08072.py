from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("status").isin(["SUCCESS", "FAILED", "REFUNDED"]))
    .groupBy("status")
    .pivot("method", ["paypal", "card"])
    .agg(F.max("amount"))
    .orderBy("status")
)
