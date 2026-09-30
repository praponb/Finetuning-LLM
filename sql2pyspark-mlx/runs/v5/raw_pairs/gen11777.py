from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("method", "order_id").orderBy("paid_at", "id")

latest = (
    df_payments
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
)

result = (
    latest
    .groupBy(F.date_trunc("YEAR", "paid_at").alias("year_start"), "status")
    .agg(
        F.count_if(F.col("method") == "paypal").alias("paypal_events"),
        F.round(F.avg("amount"), 2).alias("avg_amount"),
        F.size(F.collect_set("status")).alias("num_status"),
    )
)
