from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("order_id").isin([32, 38, 40]))
    .withColumn("method_length", F.length("method"))
)
