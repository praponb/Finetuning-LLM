from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("method") == "cash")
    .groupBy("status", F.hour("paid_at").alias("paid_hour"))
    .agg(F.sum("amount").alias("total_amount"))
)
