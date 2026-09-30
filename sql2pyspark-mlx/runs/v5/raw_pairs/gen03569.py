from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("method", "status").orderBy("paid_at", "id")

latest = (
    df_payments
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
)

result = (
    latest
    .groupBy(
        F.date_trunc("month", "paid_at").alias("activity_month"),
        F.when(F.col("amount") >= 1540, "HIGH").otherwise("LOW").alias("amount_tier"),
    )
    .agg(
        F.min("paid_at").alias("first_paid_at"),
        F.countDistinct("id").alias("distinct_ids"),
        F.avg(F.when(F.col("method") == "paypal", F.col("amount"))).alias("paypal_avg_amount"),
    )
    .filter(F.col("paypal_avg_amount") < 1000000)
    .orderBy(F.col("amount_tier").asc_nulls_first())
)
