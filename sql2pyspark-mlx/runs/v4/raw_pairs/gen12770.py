from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("amount").desc())
w2 = Window.partitionBy("status", "store_id")
w3 = Window.partitionBy("store_id")
w4 = Window.partitionBy("customer_id", "channel").orderBy("amount_rank", "id")
w5 = Window.partitionBy("customer_id").orderBy("shipping_fee", "id")
w6 = Window.partitionBy("status")
w7 = Window.orderBy("store_id_avg_amount_share", "id")
w8 = Window.partitionBy("status").orderBy(F.col("amount").desc(), "id")
w9 = Window.partitionBy("store_id", "channel").orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

enriched = (
    df_orders
    .select(
        "*",
        F.rank().over(w).alias("amount_rank"),
        F.corr("amount", "discount").over(w2).alias("corr_amount_discount"),
        F.avg("amount").over(w3).alias("store_id_avg_amount"),
    )
)

eligible = (
    enriched
    .alias("s")
    .join(df_payments.alias("ch"), (F.col("ch.order_id") == F.col("s.id")) & (F.col("ch.paid_at") <= F.col("s.order_date")), "left_anti")
)

base = (
    eligible
    .filter((F.col("order_date") > F.to_date(F.lit("2024-02-08"))) & (F.col("store_id_avg_amount") == 1373))
    .withColumn("channel_status", F.concat("channel", F.lit(" - "), "status"))
    .withColumn("channel_filled", F.coalesce("channel", F.lit("DIRECT")))
)

metrics = (
    base
    .withColumn("store_id_avg_amount_share", F.round((F.col("store_id_avg_amount") / F.sum("store_id_avg_amount").over(w3)), 4))
    .withColumn("event_seq", F.row_number().over(w4))
    .withColumn("shipping_fee_bucket", F.ntile(3).over(w5))
)

eligible_orders = (
    metrics
    .alias("s")
    .join(df_order_items.alias("ch"), (F.col("ch.order_id") == F.col("s.id")) & (F.col("ch.unit_price").between(110, 240)), "left_semi")
)

enriched_orders = (
    eligible_orders
    .select(
        "*",
        F.count("discount").over(w6).alias("status_count_discount"),
        F.lead("event_seq", 1, 0).over(w7).alias("next_event_seq"),
    )
)

enriched_orders_2 = (
    enriched_orders
    .withColumn("amount_bucket", F.ntile(10).over(w8))
    .withColumn("first_known_status_count_discount", F.first("status_count_discount", ignorenulls=True).over(w9))
)

result = (
    enriched_orders_2
    .groupBy("customer_id", F.date_trunc("MONTH", "order_date").alias("activity_month"))
    .agg(
        F.count(F.when(F.col("channel") == "web", 1)).alias("web_rows"),
        F.avg(F.when(F.col("status") == "PENDING", F.col("status_count_discount"))).alias("pending_avg_status_count_discount"),
        F.max(F.when(F.col("channel") == "store", F.col("next_event_seq"))).alias("store_max_next_event_seq"),
        F.round(F.avg("corr_amount_discount"), 2).alias("avg_corr_amount_discount"),
        F.max(F.when(F.col("status") == "SHIPPED", F.col("shipping_fee_bucket"))).alias("shipped_max_shipping_fee_bucket"),
        F.avg("status_count_discount").alias("avg_status_count_discount"),
    )
)
