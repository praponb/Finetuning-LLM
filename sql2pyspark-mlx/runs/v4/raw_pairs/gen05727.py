from pyspark.sql import functions as F

result = (
    df_payments
    .groupBy("method")
    .pivot("status", ["FAILED", "SUCCESS"])
    .agg(F.sum("amount"))
)
