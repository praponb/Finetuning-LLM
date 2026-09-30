from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id"), F.col("s.subscription_id")).orderBy(F.col("c.cost").desc(), F.col("c.id").desc())
amount_by_customer_id = df_orders.groupBy("customer_id").agg(F.max("amount").alias("max_order_amount"))

filtered = (
    df_orders
    .filter((F.col("order_date") > F.lit("2023-06-14").cast("date")) & F.col("channel").isNotNull())
    .select(
        "id",
        "customer_id",
        "store_id",
        "order_date",
        "status",
        "discount",
        "shipping_fee",
        (F.col("discount") * F.col("amount")).alias("discount_times_amount"),
    )
)

with_dims = (
    filtered
    .alias("e")
    .join(df_stores.alias("b"), F.col("e.store_id").eqNullSafe(F.col("b.id")), "left")
    .select("e.*", F.col("b.opened_date"), F.col("b.city"), F.col("b.region"))
)

matched = (
    with_dims
    .alias("a")
    .join(df_subscriptions.alias("sub"), (F.col("a.customer_id") == F.col("sub.customer_id")) & (F.col("a.order_date") >= F.col("sub.start_date")) & (F.col("a.order_date") < F.col("sub.end_date") + F.expr("INTERVAL 14 DAYS")), "left")
    .select(
        "a.*",
        F.col("sub.id").alias("subscription_id"),
        F.col("sub.plan"),
        F.col("sub.monthly_fee"),
    )
)

with_last = (
    matched
    .alias("s")
    .join(df_shipments.alias("c"), (F.col("c.order_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.customer_id",
        "s.store_id",
        "s.order_date",
        "s.shipping_fee",
        "s.discount_times_amount",
        "s.city",
        "s.region",
        "s.subscription_id",
        "s.plan",
        "s.monthly_fee",
        F.col("c.delivered_date").alias("shipment_delivered_date"),
    )
)

with_window = (
    with_last
    .alias("a")
    .join(df_subscriptions.alias("sub"), (F.col("a.customer_id") == F.col("sub.customer_id")) & (F.col("a.order_date") >= F.col("sub.start_date")) & (F.col("a.order_date") < F.col("sub.end_date") + F.expr("INTERVAL 7 DAYS")), "left")
    .select(
        "a.*",
        F.col("sub.id").alias("subscription_id_2"),
        F.col("sub.plan").alias("subscription_plan"),
        F.col("sub.monthly_fee").alias("subscription_monthly_fee"),
    )
)

result = (
    with_window
    .join(amount_by_customer_id, on="customer_id", how="left")
    .select(
        "id",
        "customer_id",
        "store_id",
        "order_date",
        "shipping_fee",
        "discount_times_amount",
        "subscription_id",
        "monthly_fee",
        "shipment_delivered_date",
        F.col("max_order_amount"),
    )
    .orderBy(F.col("customer_id").desc())
)
