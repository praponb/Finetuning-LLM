from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("order_id").isin([21, 22, 29, 32]))
    .select("id", "method", F.length("method").alias("method_length"))
)
