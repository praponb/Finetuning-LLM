from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.hour("paid_at").between(7, 10) & F.col("amount").isNotNull())
    .groupBy(F.to_date("paid_at").alias("paid_day"))
    .agg(F.max("paid_at").alias("latest_paid_at"))
    .orderBy("paid_day")
)
