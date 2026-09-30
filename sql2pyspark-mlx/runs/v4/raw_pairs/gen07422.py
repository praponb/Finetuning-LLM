from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id")
w2 = Window.partitionBy("channel", "store_id").orderBy(F.col("amount").desc(), "id")
w3 = Window.partitionBy("status").orderBy("discount")
w4 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.quantity").desc(), F.col("c.id").desc())

base = (
    df_orders
    .filter(F.col("customer_id").isNotNull() & F.col("order_date").between("2023-04-01", "2024-05-01"))
    .select(
        "*",
        F.least("discount", "shipping_fee").alias("least_discount_shipping_fee"),
    )
)

scored = (
    base
    .withColumn("store_id_max_discount", F.max("discount").over(w))
    .withColumn("amount_bucket", F.ntile(3).over(w2))
    .withColumn("discount_dense_rank", F.dense_rank().over(w3))
)

not_flagged = (
    scored
    .alias("s")
    .join(df_order_items.alias("ch"), (F.col("ch.order_id") == F.col("s.id")) & (F.col("ch.order_id").isin([32, 35, 37, 38])), "left_semi")
)

eligible = (
    not_flagged
    .alias("s")
    .join(df_shipments.alias("x"), (F.col("x.order_id") == F.col("s.id")) & (F.col("x.delivered_date") < F.col("s.order_date")), "left_semi")
)

with_latest = (
    eligible
    .alias("s")
    .join(df_order_items.alias("c"), (F.col("c.order_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w4))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.quantity").alias("order_item_quantity"))
)

matched = (
    with_latest
    .alias("src")
    .join(df_subscriptions.alias("sub"), (F.col("src.customer_id") == F.col("sub.customer_id")) & (F.col("src.order_date") >= F.col("sub.start_date")) & (F.col("src.order_date") < F.col("sub.end_date") + F.expr("INTERVAL 14 DAYS")), "left")
    .select(
        "src.*",
        F.col("sub.id").alias("subscription_id"),
        F.col("sub.plan"),
        F.col("sub.monthly_fee"),
    )
)

result = (
    matched
    .groupBy("plan", "store_id")
    .pivot("channel", ["mobile", "web"])
    .agg(F.sum("discount").alias("value_sum"), F.count(F.lit(1)).alias("cnt"))
)
