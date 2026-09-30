from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("status").isin(["REFUNDED", "FAILED", "SUCCESS"]) & (~F.col("status").isin(["FAILED", "REFUNDED"]) | (F.col("order_id") == 26)))
)
