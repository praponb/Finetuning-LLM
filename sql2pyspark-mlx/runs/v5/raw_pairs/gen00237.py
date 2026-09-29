from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.cost").desc(), F.col("c.id").desc())
w_running = Window.partitionBy("plan", "customer_id").orderBy("shipment_shipped_date", "id", "subscription_id").rowsBetween(-2, Window.currentRow)
w3 = Window.orderBy("monthly_fee", "id", "subscription_id")

valid = (
    df_orders
    .filter(F.col("order_date") >= F.to_date(F.lit("2023-03-21")))
    .select(
        "*",
        (F.col("amount") + F.col("shipping_fee")).alias("amount_with_shipping"),
        F.substring("status", 1, 2).alias("status_prefix"),
    )
)

latest_match = (
    valid
    .alias("s")
    .join(df_shipments.alias("c"), (F.col("c.order_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.customer_id",
        "s.store_id",
        "s.status",
        "s.channel",
        "s.amount",
        "s.shipping_fee",
        "s.amount_with_shipping",
        F.col("c.delivered_date"),
        F.col("c.shipped_date").alias("shipment_shipped_date"),
    )
)

with_window = (
    latest_match
    .alias("src")
    .join(df_subscriptions.alias("sub"), (F.col("src.customer_id") == F.col("sub.customer_id")) & (F.col("src.delivered_date") >= F.col("sub.start_date")) & (F.col("src.delivered_date") < F.col("sub.end_date") + F.expr("INTERVAL 7 DAYS")), "left")
    .select(
        "src.*",
        F.col("sub.id").alias("subscription_id"),
        F.col("sub.plan"),
        F.col("sub.monthly_fee"),
    )
)

metrics = (
    with_window
    .filter(F.col("status") == "SHIPPED")
    .withColumn("last3_count_amount_with_shipping", F.count("amount_with_shipping").over(w_running))
    .withColumn("last3_min_amount_with_shipping", F.min("amount_with_shipping").over(w_running))
    .withColumn("last3_sum_amount_with_shipping", F.sum("amount_with_shipping").over(w_running))
    .withColumn("prev_monthly_fee", F.lag("monthly_fee", 1).over(w3))
)

result = (
    metrics
    .select(
        "id",
        "subscription_id",
        "customer_id",
        "amount",
        "last3_min_amount_with_shipping",
        "prev_monthly_fee",
    )
    .unpivot(
        ids=["id", "subscription_id", "customer_id"],
        values=["amount", "last3_min_amount_with_shipping", "prev_monthly_fee"],
        variableColumnName="kpi",
        valueColumnName="reading",
    )
    .filter(F.col("reading").isNotNull())
)
