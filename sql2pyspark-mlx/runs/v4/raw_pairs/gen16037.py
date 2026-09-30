from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.partitionBy("status")
w2 = Window.partitionBy("status").orderBy(F.col("amount").desc_nulls_first())
w3 = Window.partitionBy("customer_id")
w4 = Window.orderBy("amount", "id")
w5 = Window.partitionBy("status", "store_id").orderBy("discount_times_shipping_fee", "id")

cleaned = (
    df_orders
    .filter(F.col("customer_id").isNotNull() & F.col("store_id").isNotNull())
    .withColumn("discount_times_shipping_fee", (F.col("discount") * F.col("shipping_fee")))
)

metrics = (
    cleaned
    .withColumn("status_sum_discount", F.sum("discount").over(w_part))
    .withColumn("amount_dense_rank", F.dense_rank().over(w2))
    .withColumn("customer_id_min_discount", F.min("discount").over(w3))
    .withColumn("next_amount", F.lead("amount", 1).over(w4))
    .withColumn("next_shipping_fee", F.lead("shipping_fee", 1).over(w5))
)

not_flagged = (
    metrics
    .alias("s")
    .join(df_shipments.alias("ch"), (F.col("ch.order_id") == F.col("s.id")) & (F.col("ch.carrier") == "Kerry"), "left_anti")
)

not_flagged_orders = (
    not_flagged
    .alias("s")
    .join(df_order_items.alias("c"), (F.col("c.order_id") == F.col("s.id")) & (F.col("c.quantity").between(4, 7)), "left_semi")
)

eligible = (
    not_flagged_orders
    .alias("s")
    .join(df_shipments.alias("ch"), (F.col("ch.order_id") == F.col("s.id")) & (F.col("ch.shipped_date") >= F.col("s.order_date")), "left_anti")
)

summary = (
    eligible
    .groupBy(F.date_trunc("WEEK", "order_date").alias("week_start"))
    .agg(
        F.avg(F.when(F.col("status") == "RETURNED", F.col("discount_times_shipping_fee"))).alias("returned_avg_discount_times_shipping_fee"),
        F.round(F.sum("customer_id_min_discount") / F.nullif(F.count(F.when(F.col("channel") == "web", 1)), F.lit(0)), 2).alias("customer_id_min_discount_per_web"),
        F.size(F.collect_set("status")).alias("num_status"),
    )
    .filter(F.col("returned_avg_discount_times_shipping_fee") >= 0)
)

result = (
    summary
    .select("week_start", "num_status")
    .orderBy(F.col("week_start").desc(), "num_status")
)
