from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id", "plan").orderBy(F.col("subscription_id").desc_nulls_first(), "id", "subscription_id")
w2 = Window.partitionBy(F.col("s.id"), F.col("s.subscription_id")).orderBy(F.col("c.amount").desc(), F.col("c.id").desc())
w3 = Window.partitionBy("channel").orderBy("rn", "id", "subscription_id").rowsBetween(-3, Window.currentRow)

base = (
    df_orders
    .filter(F.col("order_date") > F.lit("2023-08-12").cast("date"))
    .select(
        "*",
        F.last_day("order_date").alias("order_date_month_end"),
        (F.col("amount") + F.col("shipping_fee")).alias("amount_with_shipping"),
    )
)

with_window = (
    base
    .alias("e")
    .join(df_subscriptions.alias("sub"), (F.col("e.customer_id") == F.col("sub.customer_id")) & (F.col("e.order_date_month_end") >= F.col("sub.start_date")) & (F.col("e.order_date_month_end") < F.col("sub.end_date") + F.expr("INTERVAL 30 DAYS")), "inner")
    .select(
        "e.*",
        F.col("sub.id").alias("subscription_id"),
        F.col("sub.plan"),
        F.col("sub.monthly_fee"),
    )
)

ranked = (
    with_window
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
)

latest_match = (
    ranked
    .alias("s")
    .join(df_payments.alias("c"), (F.col("c.order_id") == F.col("s.id")), "left")
    .withColumn("rn_2", F.row_number().over(w2))
    .filter(F.col("rn_2") == 1)
    .select("s.*", F.col("c.paid_at").alias("payment_paid_at"), F.col("c.method"))
)

with_windows = (
    latest_match
    .filter(F.col("status").isin(["SHIPPED", "COMPLETED"]))
    .select(
        "id",
        "customer_id",
        "store_id",
        "order_date",
        "channel",
        "amount",
        "discount",
        "shipping_fee",
        "subscription_id",
        "monthly_fee",
        "rn",
        "payment_paid_at",
        F.sum("amount").over(w3).alias("last4_sum_amount"),
        F.min("amount").over(w3).alias("last4_min_amount"),
        F.max("amount").over(w3).alias("last4_max_amount"),
    )
)

totals = (
    with_windows
    .withColumn("last4_max_amount_tier", F.when(F.col("last4_max_amount") >= 1770, "HIGH").otherwise("LOW"))
    .withColumn("week_start", F.date_trunc("week", "order_date"))
    .groupingSets(
        [["last4_max_amount_tier", "week_start"], ["last4_max_amount_tier"], []],
        "last4_max_amount_tier",
        "week_start",
    )
    .agg(
        F.sort_array(F.collect_set("channel")).alias("channel_list"),
        F.max("payment_paid_at").alias("last_seen"),
        F.sum("amount").alias("total_amount"),
        F.corr("shipping_fee", "amount").alias("corr_shipping_fee_amount"),
        F.sum(F.when(F.col("channel") == "partner", F.col("amount")).otherwise(0)).alias("partner_amount"),
        F.grouping("last4_max_amount_tier").alias("grouping_id"),
    )
    .filter(F.col("corr_shipping_fee_amount") >= 0)
)

result = (
    totals
    .select("week_start", "total_amount", "corr_shipping_fee_amount", "grouping_id")
    .orderBy(F.col("grouping_id").asc_nulls_last())
)
