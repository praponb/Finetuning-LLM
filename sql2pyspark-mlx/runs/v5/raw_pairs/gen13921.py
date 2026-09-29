from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("subscription_id")
w2 = Window.partitionBy("store_id").orderBy("amount", "id", "subscription_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("customer_id").orderBy("order_date", "id", "subscription_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("store_id", "customer_id").orderBy("order_date", "id", "subscription_id")
w5 = Window.partitionBy(F.col("s.id"), F.col("s.subscription_id")).orderBy(F.col("c.amount").desc(), F.col("c.id").desc())
w6 = Window.partitionBy(F.col("s.id"), F.col("s.subscription_id")).orderBy(F.col("c.unit_price").desc(), F.col("c.id").desc())

with_window = (
    df_orders
    .alias("src")
    .join(df_subscriptions.alias("sub"), (F.col("src.customer_id") == F.col("sub.customer_id")) & (F.col("src.order_date") >= F.col("sub.start_date")) & (F.col("src.order_date") < F.col("sub.end_date") + F.expr("INTERVAL 30 DAYS")), "inner")
    .select(
        "src.*",
        F.col("sub.id").alias("subscription_id"),
        F.col("sub.plan"),
        F.col("sub.monthly_fee"),
    )
)

joined = (
    with_window
    .alias("e")
    .join(df_stores.alias("d"), F.col("e.store_id").eqNullSafe(F.col("d.id")), "left")
    .select(
        "e.id",
        "e.customer_id",
        "e.store_id",
        "e.order_date",
        "e.amount",
        "e.subscription_id",
        F.col("d.store_name"),
    )
)

enriched = (
    joined
    .withColumn("subscription_id_rank", F.rank().over(w))
    .withColumn("first_known_amount", F.first("amount", ignorenulls=True).over(w2))
    .withColumn("running_amount", F.sum("amount").over(w3))
)

latest = (
    enriched
    .withColumn("row_num", F.row_number().over(w4))
    .filter(F.col("row_num") == 1)
    .select(
        "id",
        "customer_id",
        "amount",
        "subscription_id",
        "subscription_id_rank",
        "running_amount",
    )
)

latest_match = (
    latest
    .alias("s")
    .join(df_payments.alias("c"), (F.col("c.order_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w5))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.amount",
        "s.subscription_id",
        "s.running_amount",
        F.col("c.paid_at"),
    )
)

with_last = (
    latest_match
    .alias("s")
    .join(df_order_items.alias("c"), (F.col("c.order_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w6))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.quantity"))
)

remaining_keys = (
    with_last
    .select("id")
    .intersectAll(
        df_orders
        .filter(F.col("shipping_fee") <= 29)
        .select("id")
    )
)

verified = (
    with_last
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

totals = (
    verified
    .groupBy(
        F.date_trunc("MONTH", "paid_at").alias("activity_month"),
        F.when(F.col("running_amount") >= 7230, "LARGE").otherwise("SMALL").alias("running_amount_tier"),
    )
    .agg(
        F.min("paid_at").alias("first_paid_at"),
        F.countDistinct("id").alias("distinct_ids"),
        ((F.unix_timestamp(F.max("paid_at")) - F.unix_timestamp(F.min("paid_at"))) / 60).alias("span_minutes"),
    )
)

result = (
    totals
    .select(
        "activity_month",
        "running_amount_tier",
        "first_paid_at",
        "distinct_ids",
        "span_minutes",
    )
    .orderBy(F.col("distinct_ids").asc_nulls_last())
)
