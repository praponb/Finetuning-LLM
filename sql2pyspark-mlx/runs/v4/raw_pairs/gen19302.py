from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.partitionBy("store_id", "channel").orderBy("shipping_fee")
w2 = Window.partitionBy("store_id").orderBy("order_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_part = Window.orderBy(F.col("shipping_fee").desc())
w4 = Window.orderBy("shipping_fee_filled", "id")
w5 = Window.partitionBy("channel")
shipping_fee_by_customer_id = df_orders.groupBy("customer_id").agg(F.max("shipping_fee").alias("max_order_shipping_fee"))

cleaned = (
    df_orders
    .filter(F.col("customer_id").isNotNull())
    .withColumn("channel_group", F.when(F.col("channel").isin(["mobile", "web"]), "Tier 1").otherwise("Other"))
)

filtered = (
    cleaned
    .filter(F.col("order_date") >= F.lit("2023-05-16").cast("date"))
    .withColumn("shipping_fee_filled", F.coalesce("shipping_fee", F.lit(0)))
    .withColumn("discount_plus_amount", (F.col("discount") + F.col("amount")))
    .withColumn("channel_filled", F.coalesce("channel", F.lit("N/A")))
)

metrics = (
    filtered
    .withColumn("shipping_fee_cume_dist", F.cume_dist().over(w_recent))
    .withColumn("last_known_channel", F.last("channel", ignorenulls=True).over(w2))
    .withColumn("shipping_fee_cume_dist_2", F.cume_dist().over(w_part))
    .withColumn("event_seq", F.row_number().over(w4))
    .withColumn("amount_share", F.round((F.col("amount") / F.sum("amount").over(w5)), 4))
)

result = (
    metrics
    .join(shipping_fee_by_customer_id, on="customer_id", how="left")
    .select(
        "id",
        "customer_id",
        "order_date",
        "status",
        "channel",
        "amount",
        "channel_group",
        "shipping_fee_filled",
        "discount_plus_amount",
        F.col("max_order_shipping_fee"),
    )
    .orderBy(F.col("id").desc(), "channel")
)
