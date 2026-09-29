from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("paid_at") < "2024-06-08")
    .groupBy("method")
    .pivot("status", ["REFUNDED", "SUCCESS"])
    .agg(F.count("amount"))
)
