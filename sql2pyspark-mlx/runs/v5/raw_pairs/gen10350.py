from pyspark.sql import functions as F

result = (
    df_payments
    .groupBy("method", F.date_trunc("MONTH", "paid_at").alias("paid_month"))
    .agg(F.sort_array(F.collect_set("status")).alias("status_list"))
    .orderBy(F.col("method").desc(), "paid_month")
    .limit(20)
)
