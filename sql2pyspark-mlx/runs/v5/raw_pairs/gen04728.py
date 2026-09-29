from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id"), F.col("s.subscription_id")).orderBy(F.col("c.quantity").desc(), F.col("c.id").desc())
w2 = Window.partitionBy(F.col("s.id"), F.col("s.subscription_id")).orderBy(F.col("c.unit_price").desc(), F.col("c.id").desc())
w3 = Window.partitionBy("store_id").orderBy("order_item_unit_price", "id", "subscription_id")
w_grp = Window.partitionBy("store_id").orderBy("unit_price", "id", "subscription_id")

joined = (
    df_orders
    .alias("src")
    .join(df_stores.alias("d"), F.col("src.store_id") == F.col("d.id"), "inner")
    .filter(F.col("d.city") == "Bangkok")
    .select("src.*", F.col("d.region"), F.col("d.store_name"), F.col("d.sqft"))
)

filtered = (
    joined
    .filter(F.col("order_date") > F.lit("2023-07-15").cast("date"))
    .withColumn("shipping_fee_band", F.when(F.col("shipping_fee") >= 31, "High").when(F.col("shipping_fee") >= 8, "Medium").otherwise("Low"))
    .withColumn("channel_clean", F.coalesce("channel", F.lit("UNKNOWN")))
)

matched = (
    filtered
    .alias("a")
    .join(df_subscriptions.alias("sub"), (F.col("a.customer_id") == F.col("sub.customer_id")) & F.col("a.order_date").between(F.col("sub.start_date"), F.col("sub.end_date")), "inner")
    .select(
        "a.*",
        F.col("sub.id").alias("subscription_id"),
        F.col("sub.plan"),
        F.col("sub.monthly_fee"),
    )
)

with_latest = (
    matched
    .alias("s")
    .join(df_order_items.alias("c"), (F.col("c.order_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.customer_id",
        "s.store_id",
        "s.channel",
        "s.shipping_fee",
        "s.store_name",
        "s.sqft",
        "s.shipping_fee_band",
        "s.subscription_id",
        "s.plan",
        "s.monthly_fee",
        F.col("c.quantity").alias("order_item_quantity"),
        F.col("c.unit_price").alias("order_item_unit_price"),
    )
)

cleaned = (
    with_latest
    .filter(F.col("order_item_unit_price").between(180, 270))
    .withColumn("plan_length", F.length("plan"))
)

with_last = (
    cleaned
    .alias("s")
    .join(df_order_items.alias("c"), (F.col("c.order_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.customer_id",
        "s.store_id",
        "s.shipping_fee",
        "s.store_name",
        "s.sqft",
        "s.subscription_id",
        "s.plan",
        "s.order_item_unit_price",
        "s.plan_length",
        F.col("c.quantity").alias("order_item_quantity_2"),
        F.col("c.unit_price"),
    )
)

metrics = (
    with_last
    .select(
        "id",
        "shipping_fee",
        "store_name",
        "sqft",
        "subscription_id",
        "order_item_quantity_2",
        F.ntile(5).over(w3).alias("order_item_unit_price_bucket"),
        F.lead("plan_length", 1).over(w_grp).alias("next_plan_length"),
        F.ntile(4).over(w_grp).alias("unit_price_bucket"),
    )
)

result = (
    metrics
    .select(
        "id",
        "shipping_fee",
        "store_name",
        "sqft",
        "subscription_id",
        "order_item_quantity_2",
        "order_item_unit_price_bucket",
        "next_plan_length",
    )
)
