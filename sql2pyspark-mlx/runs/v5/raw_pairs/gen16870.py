from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id"), F.col("s.subscription_id"), F.col("s.promo_id")).orderBy(F.col("c.amount").desc(), F.col("c.id").desc())

kept = (
    df_orders
    .alias("s")
    .join(df_shipments.alias("ch"), (F.col("ch.order_id") == F.col("s.id")) & (F.col("ch.shipped_date") < F.col("s.order_date")), "left_anti")
)

matched = (
    kept
    .alias("src")
    .join(df_subscriptions.alias("sub"), (F.col("src.customer_id") == F.col("sub.customer_id")) & (F.col("src.order_date") >= F.col("sub.start_date")) & (F.col("src.order_date") < F.col("sub.end_date") + F.expr("INTERVAL 7 DAYS")), "left")
    .select(
        "src.*",
        F.col("sub.id").alias("subscription_id"),
        F.col("sub.plan"),
        F.col("sub.monthly_fee"),
    )
)

attributed = (
    matched
    .alias("s")
    .join(df_promotions.alias("pr"), (F.col("s.channel") == F.col("pr.channel")) & F.col("s.order_date").between(F.col("pr.starts_on"), F.col("pr.ends_on")), "left")
    .select(
        "s.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code"),
        F.col("pr.discount_pct"),
    )
)

with_latest = (
    attributed
    .alias("s")
    .join(df_payments.alias("c"), (F.col("c.order_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.customer_id",
        "s.order_date",
        "s.channel",
        "s.shipping_fee",
        "s.subscription_id",
        "s.plan",
        "s.monthly_fee",
        "s.promo_id",
        F.col("c.amount").alias("payment_amount"),
        F.col("c.method").alias("payment_method"),
    )
)

result = (
    with_latest
    .alias("s")
    .join(df_customers.alias("p"), F.col("s.customer_id") == F.col("p.id"), "inner")
    .groupingSets(
        [["country", "channel"], ["country"], []],
        F.col("p.country").alias("country"),
        F.col("s.channel").alias("channel"),
    )
    .agg(
        F.count("*").alias("num_rows"),
        F.round(F.sum(F.col("s.payment_amount")), 2).alias("total_payment_amount"),
        F.max(F.col("s.subscription_id")).alias("max_subscription_id"),
        F.grouping_id().alias("level_id"),
    )
)
