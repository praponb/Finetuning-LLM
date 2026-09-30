from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.quantity").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("store_id", "customer_id").orderBy("discount", "id").rowsBetween(-6, Window.currentRow)
w3 = Window.partitionBy("customer_id").orderBy("amount_filled", "id").rowsBetween(-5, Window.currentRow)
w_grp = Window.orderBy("amount", "id")
w5 = Window.partitionBy("channel")
w6 = Window.partitionBy("customer_id").orderBy("shipping_fee", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.paid_at").desc(), F.col("c.id").desc())
w8 = Window.orderBy(F.col("amount_filled").asc_nulls_last())
w9 = Window.partitionBy("payment_method").orderBy("discount_avg_6", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_orders
    .filter(~F.col("status").isin(["CANCELLED", "COMPLETED"]))
    .select(
        "*",
        F.upper("channel").alias("channel_upper"),
        F.coalesce("amount", F.lit(0)).alias("amount_filled"),
    )
)

base = (
    filtered
    .filter(F.col("order_date") >= F.to_date(F.lit("2023-06-24")))
    .select(
        "*",
        F.upper("status").alias("status_upper"),
        F.when(F.col("discount") >= 0.31, "High").when(F.col("discount") >= 0.06, "Medium").otherwise("Low").alias("discount_band"),
        F.coalesce("channel", F.lit("DIRECT")).alias("resolved_channel"),
    )
)

latest_match = (
    base
    .alias("s")
    .join(df_order_items.alias("c"), (F.col("c.order_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.unit_price").alias("order_item_unit_price"),
        F.col("c.quantity"),
    )
)

metrics = (
    latest_match
    .withColumn("discount_avg_6", F.avg("discount").over(w2))
    .withColumn("quantity_avg_5", F.avg("quantity").over(w3))
    .withColumn("amount_bucket", F.ntile(3).over(w_grp))
    .withColumn("channel_count_amount", F.count("amount").over(w5))
)

enriched = (
    metrics
    .withColumn("running_sum_channel_count_amount", F.sum("channel_count_amount").over(w6))
    .withColumn("running_count_channel_count_amount", F.count("channel_count_amount").over(w6))
    .withColumn("running_min_channel_count_amount", F.min("channel_count_amount").over(w6))
    .withColumn("running_max_channel_count_amount", F.max("channel_count_amount").over(w6))
)

with_last = (
    enriched
    .alias("s")
    .join(df_payments.alias("c"), (F.col("c.order_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w7))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.customer_id",
        "s.order_date",
        "s.status",
        "s.amount",
        "s.discount",
        "s.shipping_fee",
        "s.channel_upper",
        "s.amount_filled",
        "s.resolved_channel",
        "s.order_item_unit_price",
        "s.quantity",
        "s.discount_avg_6",
        "s.amount_bucket",
        "s.channel_count_amount",
        "s.running_count_channel_count_amount",
        "s.running_min_channel_count_amount",
        "s.running_max_channel_count_amount",
        F.col("c.method").alias("payment_method"),
        F.col("c.paid_at"),
    )
)

result = (
    with_last
    .filter(F.col("running_max_channel_count_amount").between(11, 16.1))
    .select(
        "id",
        "order_date",
        "status",
        "amount",
        "shipping_fee",
        "channel_upper",
        "amount_filled",
        "resolved_channel",
        "quantity",
        "running_count_channel_count_amount",
        "running_min_channel_count_amount",
        "payment_method",
        "paid_at",
        F.dense_rank().over(w8).alias("amount_filled_dense_rank"),
        F.last("order_item_unit_price", ignorenulls=True).over(w9).alias("last_known_order_item_unit_price"),
    )
)
