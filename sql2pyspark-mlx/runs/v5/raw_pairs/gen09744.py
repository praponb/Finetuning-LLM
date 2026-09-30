from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.cost").desc(), F.col("c.id").desc())
w_ordered = Window.partitionBy("store_id", "plan").orderBy("shipment_shipped_date", "id", "subscription_id")
w3 = Window.partitionBy("status").orderBy(F.col("amount").desc(), "id", "subscription_id")
w4 = Window.partitionBy("country").orderBy(F.col("amount").desc(), "id", "subscription_id")
w5 = Window.partitionBy("country").orderBy(F.col("subscription_id").desc(), "id", "subscription_id")
w6 = Window.partitionBy("status").orderBy("next_monthly_fee", "id", "subscription_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_orders
    .filter(F.col("status").isin(["COMPLETED", "SHIPPED", "PENDING"]))
    .withColumn("discount_int", F.col("discount").cast("int"))
)

with_latest = (
    base
    .alias("s")
    .join(df_shipments.alias("c"), (F.col("c.order_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.delivered_date"),
        F.col("c.shipped_date").alias("shipment_shipped_date"),
    )
)

not_flagged = (
    with_latest
    .alias("s")
    .join(df_order_items.alias("c"), (F.col("c.order_id") == F.col("s.id")) & (F.col("c.quantity") < 4), "left_semi")
)

attributed = (
    not_flagged
    .alias("src")
    .join(df_subscriptions.alias("sub"), (F.col("src.customer_id") == F.col("sub.customer_id")) & (F.col("src.order_date") >= F.col("sub.start_date")) & (F.col("src.order_date") < F.col("sub.end_date") + F.expr("INTERVAL 30 DAYS")), "left")
    .select(
        "src.*",
        F.col("sub.id").alias("subscription_id"),
        F.col("sub.plan"),
        F.col("sub.monthly_fee"),
    )
)

with_windows = (
    attributed
    .select(
        "id",
        "customer_id",
        "order_date",
        "status",
        "channel",
        "amount",
        "shipment_shipped_date",
        "subscription_id",
        "monthly_fee",
        F.lead("discount").over(w_ordered).alias("next_discount"),
        F.lag("discount").over(w_ordered).alias("prev_discount"),
        F.lag("monthly_fee").over(w_ordered).alias("prev_monthly_fee"),
        F.lead("monthly_fee").over(w_ordered).alias("next_monthly_fee"),
        F.row_number().over(w_ordered).alias("event_seq"),
    )
)

joined = (
    with_windows
    .alias("a")
    .join(df_customers.alias("b"), F.col("a.customer_id") == F.col("b.id"), "inner")
    .select(
        "a.id",
        "a.customer_id",
        "a.status",
        "a.amount",
        "a.subscription_id",
        "a.prev_discount",
        "a.next_monthly_fee",
        F.col("b.segment"),
        F.col("b.name"),
        F.col("b.country"),
    )
)

deduped = (
    joined
    .withColumn("row_num", F.row_number().over(w3))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

result = (
    deduped
    .withColumn("amount_bucket", F.ntile(10).over(w4))
    .withColumn("subscription_id_bucket", F.ntile(10).over(w5))
    .withColumn("running_next_monthly_fee", F.sum("next_monthly_fee").over(w6))
)
