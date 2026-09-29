from pyspark.sql import functions as F

result = (
    df_payments
    .groupBy("method")
    .pivot("status", ["REFUNDED", "FAILED", "SUCCESS"])
    .agg(F.max("amount").alias("value_sum"))
)
