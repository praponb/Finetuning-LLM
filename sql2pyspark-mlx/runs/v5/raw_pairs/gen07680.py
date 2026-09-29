from pyspark.sql import functions as F

result = (
    df_payments
    .groupBy(F.date_format("paid_at", "yyyy-MM").alias("paid_month"), "status")
    .agg(F.count("*").alias("row_count"))
    .filter(F.col("row_count") > 6)
    .orderBy("paid_month")
)
