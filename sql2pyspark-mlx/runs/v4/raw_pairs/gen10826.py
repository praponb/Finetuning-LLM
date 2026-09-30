from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.partitionBy("status")
w2 = Window.partitionBy("store_id").orderBy("order_date", "id").rowsBetween(-5, Window.currentRow)
w3 = Window.partitionBy("channel", "customer_id").orderBy("channel_length", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("customer_id").orderBy("order_date", "id")

base = (
    df_orders
    .filter((F.col("order_date") >= F.lit("2024-01-17").cast("date")) & F.col("store_id").isNotNull())
    .withColumn("discount_whole", F.ceil("discount"))
    .withColumn("channel_length", F.length("channel"))
    .withColumn("channel_filled", F.coalesce("channel", F.lit("UNKNOWN")))
)

result = (
    base
    .withColumn("corr_amount_shipping_fee", F.corr("amount", "shipping_fee").over(w_running))
    .withColumn("discount_avg_5", F.avg("discount").over(w2))
    .withColumn("cumulative_discount_whole", F.sum("discount_whole").over(w3))
    .withColumn("seq_no", F.row_number().over(w4))
    .withColumn("status_sum_channel_length", F.sum("channel_length").over(w_running))
)
