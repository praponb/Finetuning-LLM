from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("order_id").isin([3, 9, 11, 40]))
    .withColumn("amount_rounded", F.round("amount", 0))
)
