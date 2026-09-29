from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.amount").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("status").orderBy(F.col("shipping_fee").asc_nulls_last())
w3 = Window.partitionBy(F.col("s.id"), F.col("s.subscription_id")).orderBy(F.col("c.unit_price").desc(), F.col("c.id").desc())
w4 = Window.partitionBy("plan").orderBy(F.col("distinct_plans").desc())

cleaned = (
    df_orders
    .filter((~F.col("channel").eqNullSafe(F.lit("web"))) & F.col("channel").isin(["mobile", "store"]))
    .select(
        "*",
        (F.col("amount") * F.col("discount")).alias("amount_times_discount"),
        F.col("shipping_fee").cast("int").alias("shipping_fee_int"),
        F.coalesce("channel", F.lit("N/A")).alias("resolved_channel"),
    )
)

filtered = (
    cleaned
    .filter(F.col("shipping_fee") <= 11)
    .select(
        "id",
        "customer_id",
        "store_id",
        "order_date",
        "status",
        "amount",
        "discount",
        "shipping_fee",
        "amount_times_discount",
        "shipping_fee_int",
        "resolved_channel",
        F.col("amount_times_discount").cast("int").alias("amount_times_discount_int"),
        F.round((F.col("amount_times_discount") * 1.07), 2).alias("amount_times_discount_scaled"),
    )
)

with_dims = (
    filtered
    .alias("src")
    .join(df_customers.alias("c"), F.col("src.customer_id") == F.col("c.id"), "inner")
    .select("src.*", F.col("c.name"), F.col("c.age"))
)

with_last = (
    with_dims
    .alias("s")
    .join(df_payments.alias("c"), (F.col("c.order_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.method").alias("payment_method"), F.col("c.paid_at"))
)

ranked = (
    with_last
    .withColumn("latest_rank", F.rank().over(w2))
    .filter(F.col("latest_rank") == 1)
    .drop("latest_rank")
)

attributed = (
    ranked
    .alias("a")
    .join(df_subscriptions.alias("sub"), (F.col("a.customer_id") == F.col("sub.customer_id")) & (F.col("a.order_date") >= F.col("sub.start_date")) & (F.col("a.order_date") <= F.date_add(F.col("sub.end_date"), 7)), "inner")
    .select(
        "a.*",
        F.col("sub.id").alias("subscription_id"),
        F.col("sub.plan"),
        F.col("sub.monthly_fee"),
    )
)

with_latest = (
    attributed
    .alias("s")
    .join(df_order_items.alias("c"), (F.col("c.order_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.customer_id",
        "s.store_id",
        "s.amount",
        "s.discount",
        "s.shipping_fee",
        "s.amount_times_discount",
        "s.shipping_fee_int",
        "s.resolved_channel",
        "s.amount_times_discount_scaled",
        "s.name",
        "s.subscription_id",
        "s.plan",
        "s.monthly_fee",
        F.col("c.quantity").alias("order_item_quantity"),
    )
)

result = (
    with_latest
    .groupBy("plan", "store_id")
    .agg(
        F.sort_array(F.collect_set("plan")).alias("plan_list"),
        F.countDistinct("plan").alias("distinct_plans"),
    )
    .withColumn("distinct_plans_rank", F.rank().over(w4))
    .orderBy(F.col("plan").desc_nulls_first())
)
