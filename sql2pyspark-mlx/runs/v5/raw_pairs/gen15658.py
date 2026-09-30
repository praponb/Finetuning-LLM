from pyspark.sql import functions as F

result = (
    df_payments
    .filter((F.col("amount") > 880) | F.col("method").isin(["paypal", "bank_transfer"]))
    .groupBy("status")
    .agg(F.min("paid_at").alias("min_paid_at"))
    .orderBy("status")
)
