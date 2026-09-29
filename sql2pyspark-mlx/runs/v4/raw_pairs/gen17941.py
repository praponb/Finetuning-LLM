from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.quantity").desc(), F.col("c.id").desc())
monthly_fee_by_customer_id = df_subscriptions.groupBy("customer_id").agg(F.sum("monthly_fee").alias("sum_subscription_monthly_fee"))

base = (
    df_orders
    .filter((F.col("order_date") > F.lit("2023-06-06").cast("date")) & (F.col("amount") > 1520))
    .select(
        "*",
        F.concat_ws("-", "status", "channel").alias("status_channel_key"),
        F.round("shipping_fee", 2).alias("shipping_fee_rounded"),
    )
)

joined = (
    base
    .alias("a")
    .join(df_stores.alias("p"), F.col("a.store_id") == F.col("p.id"), "left")
    .select("a.*", F.col("p.sqft"), F.col("p.store_name"))
)

with_latest = (
    joined
    .alias("s")
    .join(df_order_items.alias("c"), (F.col("c.order_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.unit_price"))
)

result = (
    with_latest
    .join(monthly_fee_by_customer_id, on="customer_id", how="left")
    .select(
        "id",
        "customer_id",
        "store_id",
        "order_date",
        "status",
        "channel",
        "amount",
        "discount",
        "shipping_fee",
        F.col("sum_subscription_monthly_fee"),
    )
    .orderBy("customer_id", "sum_subscription_monthly_fee")
)
