from pyspark.sql import functions as F

result = (
    df_payments
    .groupBy("method")
    .pivot("status", ["REFUNDED", "FAILED"])
    .agg(F.max("amount").alias("value_sum"), F.count(F.lit(1)).alias("cnt"))
)
