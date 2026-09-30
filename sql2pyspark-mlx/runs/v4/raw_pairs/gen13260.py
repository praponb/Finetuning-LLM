from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy("shipping_fee", "id").rowsBetween(-6, -1)
w2 = Window.orderBy("shipping_fee", "id")
w3 = Window.orderBy("shipping_fee", "id").rowsBetween(-3, -1)
w4 = Window.partitionBy("customer_id").orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_grp = Window.partitionBy("channel").orderBy("amount")
w_part = Window.partitionBy("status").orderBy("amount_rank", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy("store_id")
w8 = Window.partitionBy("status").orderBy(F.col("discount_avg_6_prior").desc(), "id")
w9 = Window.partitionBy("channel").orderBy("prev_discount", "id").rowsBetween(-5, Window.currentRow)
w_hist = Window.orderBy("discount_avg_6_prior", "id")
w11 = Window.partitionBy("status", "channel")
w12 = Window.orderBy(F.col("discount").desc())
w13 = Window.orderBy(F.col("shipping_fee").desc_nulls_last())

scored = (
    df_orders
    .withColumn("discount_avg_6_prior", F.avg("discount").over(w))
    .withColumn("prev_discount", F.lag("discount", 1).over(w2))
    .withColumn("amount_avg_3_prior", F.avg("amount").over(w3))
    .withColumn("first_known_shipping_fee", F.first("shipping_fee", ignorenulls=True).over(w4))
    .withColumn("amount_rank", F.rank().over(w_grp))
)

metrics = (
    scored
    .withColumn("last_known_amount_avg_3_prior", F.last("amount_avg_3_prior", ignorenulls=True).over(w_part))
    .withColumn("store_id_stddev_samp_shipping_fee", F.stddev_samp("shipping_fee").over(w7))
    .withColumn("discount_avg_6_prior_bucket", F.ntile(5).over(w8))
    .withColumn("amount_avg_5", F.avg("amount").over(w9))
    .withColumn("row_seq", F.row_number().over(w_hist))
)

not_flagged = (
    metrics
    .alias("s")
    .join(df_payments.alias("c"), (F.col("c.order_id") == F.col("s.id")) & (F.col("c.paid_at") <= F.col("s.order_date")), "left_anti")
)

with_dims = (
    not_flagged
    .alias("e")
    .join(df_customers.alias("c"), F.col("e.customer_id") == F.col("c.id"), "inner")
    .select("e.*", F.col("c.name"))
)

matched_keys = (
    with_dims
    .select("status", "channel")
    .intersect(
        df_orders
        .filter(F.col("status").isin(["PENDING", "CANCELLED", "RETURNED"]))
        .select("status", "channel")
    )
)

verified = (
    with_dims
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.status") == F.col("k.status")) & (F.col("s.channel") == F.col("k.channel")), "inner")
    .select("s.*")
)

metrics_orders = (
    verified
    .select(
        "*",
        F.min("first_known_shipping_fee").over(w11).alias("status_min_first_known_shipping_fee"),
        F.percent_rank().over(w12).alias("discount_percent_rank"),
        F.dense_rank().over(w13).alias("shipping_fee_dense_rank"),
    )
)

eligible = (
    metrics_orders
    .alias("s")
    .join(df_payments.alias("x"), (F.col("x.order_id") == F.col("s.id")) & (F.col("x.order_id").isin([12, 14, 27, 34])), "left_anti")
)

result = (
    eligible
    .groupBy("store_id")
    .pivot("channel", ["store", "mobile"])
    .agg(F.sum("amount_avg_3_prior").alias("value_sum"))
)
