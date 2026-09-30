from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy(F.col("shipping_fee").desc_nulls_first())
w2 = Window.partitionBy("channel")
w_part = Window.orderBy("order_date", "id").rowsBetween(-2, Window.currentRow)
w_running = Window.partitionBy("channel", "customer_id").orderBy("order_date", "id").rowsBetween(-2, Window.currentRow)
w5 = Window.orderBy("amount", "id")

with_windows = (
    df_orders
    .select(
        "*",
        F.percent_rank().over(w).alias("shipping_fee_percent_rank"),
        F.avg("amount").over(w2).alias("channel_avg_amount"),
        F.avg("discount").over(w_part).alias("discount_avg_2"),
        F.avg("shipping_fee").over(w_running).alias("shipping_fee_avg_2"),
        F.row_number().over(w5).alias("event_seq"),
    )
)

result = (
    with_windows
    .groupBy(F.date_trunc("MONTH", "order_date").alias("activity_month"))
    .agg(
        F.min("discount_avg_2").alias("min_discount_avg_2"),
        F.avg("amount").alias("avg_amount"),
        F.sum(F.when(F.col("status") == "RETURNED", F.col("discount"))).alias("returned_total_discount"),
        F.count(F.when(F.col("channel") == "partner", 1)).alias("partner_rows"),
    )
)
