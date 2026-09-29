from pyspark.sql import functions as F

filtered_payments = (
    df_payments
    .filter(F.to_date("paid_at") >= "2024-10-01")
    .withColumn("paid_at_year", F.year("paid_at"))
)

status_summary = (
    filtered_payments
    .groupBy("status")
    .agg(F.max("paid_at_year").alias("max_paid_at_year"))
)

result = (
    status_summary
    .orderBy("status")
    .limit(10)
)
