from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("paid_at") >= "2024-10-01")
    .withColumn("is_paypal", F.when(F.col("method") == "paypal", 1).otherwise(0))
)
