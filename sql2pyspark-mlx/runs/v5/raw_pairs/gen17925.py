from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.delivered_date").desc(), F.col("c.id").desc())
w2 = Window.partitionBy(F.col("s.id"), F.col("s.subscription_id")).orderBy(F.col("c.delivered_date").desc(), F.col("c.id").desc())

latest_match = (
    df_orders
    .alias("s")
    .join(df_shipments.alias("c"), (F.col("c.order_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.customer_id",
        "s.status",
        "s.channel",
        "s.amount",
        "s.discount",
        "s.shipping_fee",
        F.col("c.shipped_date"),
    )
)

matched = (
    latest_match
    .alias("a")
    .join(df_subscriptions.alias("sub"), (F.col("a.customer_id") == F.col("sub.customer_id")) & (F.col("a.shipped_date") >= F.col("sub.start_date")) & (F.col("a.shipped_date") < F.col("sub.end_date") + F.expr("INTERVAL 30 DAYS")), "left")
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
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.carrier"), F.col("c.cost").alias("shipment_cost"))
)

eligible = (
    with_last
    .alias("s")
    .join(df_payments.alias("x"), (F.col("x.order_id") == F.col("s.id")) & (F.col("x.paid_at") >= F.col("s.shipped_date")), "left_anti")
)

result = (
    eligible
    .alias("src")
    .join(df_customers.alias("dim"), F.col("src.customer_id") == F.col("dim.id"), "inner")
    .cube(F.col("dim.country").alias("country"), F.col("src.plan").alias("plan"))
    .agg(
        F.count("*").alias("cnt"),
        F.max(F.col("src.shipping_fee")).alias("max_shipping_fee"),
    )
)
