from pyspark.sql import functions as F

result = (
    df_payments
    .filter((F.col("order_id") == 20) | F.hour("paid_at").between(12, 19))
    .agg(F.min("amount").alias("min_amount"))
    .orderBy(F.col("min_amount").desc())
)
