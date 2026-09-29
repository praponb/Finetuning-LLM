from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("amount", "id")
w2 = Window.partitionBy(F.col("s.id"), F.col("s.subscription_id")).orderBy(F.col("c.quantity").desc(), F.col("c.id").desc())
w3 = Window.partitionBy("customer_id").orderBy(F.col("age").desc())

deduped = (
    df_orders
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
)

attributed = (
    deduped
    .alias("a")
    .join(df_subscriptions.alias("sub"), (F.col("a.customer_id") == F.col("sub.customer_id")) & F.col("a.order_date").between(F.col("sub.start_date"), F.col("sub.end_date")), "inner")
    .select(
        "a.*",
        F.col("sub.id").alias("subscription_id"),
        F.col("sub.plan"),
        F.col("sub.monthly_fee"),
    )
)

joined = (
    attributed
    .alias("src")
    .join(df_customers.alias("p"), F.col("src.customer_id").eqNullSafe(F.col("p.id")), "inner")
    .filter(~F.col("p.city").isin(["Chiang Mai", "Berlin"]))
    .select(
        "src.id",
        "src.customer_id",
        "src.order_date",
        "src.status",
        "src.channel",
        "src.discount",
        "src.shipping_fee",
        "src.subscription_id",
        "src.monthly_fee",
        F.col("p.email"),
        F.col("p.city"),
        F.col("p.age"),
    )
)

latest_match = (
    joined
    .alias("s")
    .join(df_order_items.alias("c"), (F.col("c.order_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.customer_id",
        "s.order_date",
        "s.channel",
        "s.shipping_fee",
        "s.subscription_id",
        "s.monthly_fee",
        "s.email",
        "s.age",
        F.col("c.unit_price"),
    )
)

deduped_orders = (
    latest_match
    .withColumn("rn", F.rank().over(w3))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

not_flagged = (
    deduped_orders
    .alias("s")
    .join(df_order_items.alias("x"), (F.col("x.order_id") == F.col("s.id")) & (F.col("x.product_id") == 3), "left_anti")
)

result = (
    not_flagged
    .alias("s")
    .join(df_customers.alias("d"), F.col("s.customer_id") == F.col("d.id"), "inner")
    .groupingSets(
        [["segment", "channel"], ["segment"], []],
        F.col("d.segment").alias("segment"),
        F.col("s.channel").alias("channel"),
    )
    .agg(
        F.count("*").alias("cnt"),
        F.min(F.col("s.shipping_fee")).alias("min_shipping_fee"),
        F.min(F.col("s.unit_price")).alias("min_unit_price"),
        F.grouping_id().alias("level_id"),
    )
)
