from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel").orderBy(F.col("adjusted_shipping_fee").desc(), "id")
w2 = Window.partitionBy("store_id").orderBy("amount", "id")
w3 = Window.orderBy("shipping_fee", "id").rowsBetween(-6, -1)
w4 = Window.partitionBy("channel", "store_id").orderBy("discount", "id")
w5 = Window.partitionBy("store_id").orderBy("order_date", "id").rowsBetween(-2, Window.currentRow)
w6 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.quantity").desc(), F.col("c.id").desc())
monthly_fee_by_customer_id = df_subscriptions.groupBy("customer_id").agg(F.sum("monthly_fee").alias("sum_subscription_monthly_fee"))

valid = (
    df_orders
    .filter(F.col("order_date") > F.to_date(F.lit("2024-03-15")))
    .withColumn("adjusted_shipping_fee", F.round((F.col("shipping_fee") * 100), 2))
)

enriched = (
    valid
    .alias("e")
    .join(df_stores.alias("d"), F.col("e.store_id") == F.col("d.id"), "inner")
    .select("e.*", F.col("d.opened_date"))
)

deduped = (
    enriched
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
)

enriched_orders = (
    deduped
    .select(
        "*",
        F.ntile(4).over(w2).alias("amount_bucket"),
        F.avg("shipping_fee").over(w3).alias("shipping_fee_avg_6_prior"),
        F.lead("amount", 1).over(w4).alias("next_amount"),
        F.avg("shipping_fee").over(w5).alias("shipping_fee_avg_2"),
    )
)

with_last = (
    enriched_orders
    .alias("s")
    .join(df_order_items.alias("c"), (F.col("c.order_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w6))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.unit_price").alias("order_item_unit_price"))
)

result = (
    with_last
    .join(monthly_fee_by_customer_id, on="customer_id", how="left")
    .select(
        "id",
        "customer_id",
        "order_date",
        "channel",
        "amount",
        "shipping_fee",
        "adjusted_shipping_fee",
        "row_num",
        "amount_bucket",
        F.when(F.col("channel").rlike("^(web|mobile)$"), "PRIORITY").when(F.col("channel").rlike("^(store|partner)$"), "CORE").otherwise("EXTENDED").alias("channel_zone"),
        F.col("sum_subscription_monthly_fee"),
    )
)
