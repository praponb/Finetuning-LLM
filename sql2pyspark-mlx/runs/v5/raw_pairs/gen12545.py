from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.delivered_date").desc(), F.col("c.id").desc())

joined = (
    df_orders
    .alias("e")
    .join(df_customers.alias("p"), F.col("e.customer_id") == F.col("p.id"), "left")
    .select(
        "e.id",
        "e.customer_id",
        "e.status",
        "e.channel",
        "e.amount",
        "e.discount",
        "e.shipping_fee",
        F.col("p.age"),
        F.col("p.city"),
        F.col("p.email"),
    )
)

eligible = (
    joined
    .alias("s")
    .join(df_order_items.alias("x"), (F.col("x.order_id") == F.col("s.id")) & (F.col("x.product_id").isNotNull()), "left_semi")
)

not_flagged = (
    eligible
    .alias("s")
    .join(df_shipments.alias("c"), (F.col("c.order_id") == F.col("s.id")) & (F.col("c.cost") > 33), "left_anti")
)

latest_match = (
    not_flagged
    .alias("s")
    .join(df_shipments.alias("c"), (F.col("c.order_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.status",
        "s.channel",
        "s.discount",
        "s.age",
        "s.city",
        "s.email",
        F.col("c.shipped_date").alias("shipment_shipped_date"),
        F.col("c.carrier").alias("shipment_carrier"),
    )
)

stats = (
    latest_match
    .groupBy(F.date_trunc("WEEK", "shipment_shipped_date").alias("week_start"))
    .agg(
        F.countDistinct("channel").alias("distinct_channels"),
        F.stddev_samp("discount").alias("discount_stddev"),
        F.sum(F.when(F.col("status") == "SHIPPED", F.col("age")).otherwise(0)).alias("shipped_age"),
        F.sum(F.when(F.col("channel") == "mobile", F.col("age")).otherwise(0)).alias("mobile_age"),
    )
)

result = (
    stats
    .select(
        "week_start",
        "distinct_channels",
        "discount_stddev",
        "shipped_age",
        "mobile_age",
    )
    .orderBy("week_start")
    .limit(10)
)
