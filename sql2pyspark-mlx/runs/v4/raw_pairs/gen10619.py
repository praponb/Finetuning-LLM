from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("order_id").isNull())
    .groupBy(F.date_trunc("MONTH", "paid_at").alias("paid_month"), "status")
    .agg(F.count("*").alias("row_count"))
    .orderBy(F.col("status").desc(), "paid_month")
)
