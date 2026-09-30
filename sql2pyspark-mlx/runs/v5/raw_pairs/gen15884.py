from pyspark.sql import functions as F

recent_payments = (
    df_payments
    .filter(F.col("order_id").isNotNull())
    .withColumn("amount_band", F.when(F.col("amount") >= 1530, "High").when(F.col("amount") >= 590, "Medium").otherwise("Low"))
)

agg = (
    recent_payments
    .groupBy(F.date_format("paid_at", "yyyy-MM").alias("paid_month"))
    .agg(
        F.min("paid_at").alias("min_paid_at"),
        F.max("paid_at").alias("last_paid_at"),
    )
)

result = agg.orderBy("last_paid_at", "min_paid_at")
