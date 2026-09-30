from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.orderBy(F.col("discount").desc(), "id")
w2 = Window.partitionBy("store_id").orderBy("order_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_hist = Window.partitionBy("store_id", "status").orderBy(F.col("discount").desc(), "id")
w4 = Window.partitionBy("status")
w5 = Window.partitionBy("store_id").orderBy("discount_bucket", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("customer_id", "status").orderBy("status_min_discount", "id").rowsBetween(-3, Window.currentRow)
w7 = Window.partitionBy("store_id").orderBy("event_seq", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.partitionBy("store_id").orderBy("discount", "id").rowsBetween(-3, -1)
w9 = Window.partitionBy("customer_id").orderBy("order_date", "id")

valid = (
    df_orders
    .filter((F.col("order_date") >= F.lit("2023-03-03").cast("date")) & (F.col("order_date") <= "2023-04-01"))
    .select(
        "id",
        "customer_id",
        "store_id",
        "order_date",
        "status",
        "discount",
        F.concat_ws(",", "status", "channel").alias("status_channel_key"),
        F.concat_ws("/", "status", "channel").alias("status_channel_key_2"),
        F.coalesce("channel", F.lit("OTHER")).alias("channel_clean"),
    )
)

with_windows = (
    valid
    .withColumn("event_seq", F.row_number().over(w_grp))
    .withColumn("running_discount", F.sum("discount").over(w2))
    .withColumn("discount_bucket", F.ntile(3).over(w_hist))
    .withColumn("status_min_discount", F.min("discount").over(w4))
)

enriched = (
    with_windows
    .withColumn("last_known_discount_bucket", F.last("discount_bucket", ignorenulls=True).over(w5))
    .withColumn("status_min_discount_avg_3", F.avg("status_min_discount").over(w6))
)

kept = (
    enriched
    .alias("s")
    .join(df_payments.alias("ch"), (F.col("ch.order_id") == F.col("s.id")) & (F.col("ch.paid_at") >= F.col("s.order_date")), "left_anti")
)

kept_orders = (
    kept
    .alias("s")
    .join(df_order_items.alias("c"), (F.col("c.order_id") == F.col("s.id")) & (F.col("c.product_id").isNull()), "left_semi")
)

with_windows_orders = (
    kept_orders
    .filter(F.col("discount_bucket") < 5)
    .withColumn("running_max_discount", F.max("discount").over(w7))
    .withColumn("running_sum_discount", F.sum("discount").over(w7))
    .withColumn("running_avg_discount", F.avg("discount").over(w7))
    .withColumn("last_known_discount_bucket_avg_3_prior", F.avg("last_known_discount_bucket").over(w8))
)

not_flagged = (
    with_windows_orders
    .alias("s")
    .join(df_shipments.alias("c"), (F.col("c.order_id") == F.col("s.id")) & (F.col("c.shipped_date") <= F.col("s.order_date")), "left_semi")
)

result = (
    not_flagged
    .withColumn("next_running_discount", F.lead("running_discount").over(w9))
    .withColumn("next_status_min_discount", F.lead("status_min_discount").over(w9))
    .withColumn("row_seq", F.row_number().over(w9))
)
