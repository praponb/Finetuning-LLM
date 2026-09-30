from pyspark.sql import functions as F

result = (
    df_payments
    .groupBy("method")
    .pivot("status", ["SUCCESS", "FAILED"])
    .agg(F.sum("amount"))
)
