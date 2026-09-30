from pyspark.sql import functions as F

valid_payments = (
    df_payments
    .filter(F.col("order_id") == 2)
    .withColumn("status_group", F.when(F.col("status").isin(["FAILED", "SUCCESS"]), "Tier 1").otherwise("Other"))
)

payments_stats = (
    valid_payments
    .groupBy("status")
    .agg(F.avg("amount").alias("avg_amount"))
)

result = payments_stats.select("status", "avg_amount")
