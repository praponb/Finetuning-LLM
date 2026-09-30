from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id"), F.col("s.promo_id")).orderBy(F.col("c.amount").desc(), F.col("c.id").desc())
w2 = Window.partitionBy(F.col("s.id"), F.col("s.promo_id")).orderBy(F.col("c.quantity").desc(), F.col("c.id").desc())

with_window = (
    df_orders
    .alias("src")
    .join(df_promotions.alias("pr"), (F.col("src.channel") == F.col("pr.channel")) & F.col("src.order_date").between(F.col("pr.starts_on"), F.col("pr.ends_on")), "inner")
    .select(
        "src.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code"),
        F.col("pr.discount_pct"),
    )
)

eligible = (
    with_window
    .alias("s")
    .join(df_order_items.alias("ch"), (F.col("ch.order_id") == F.col("s.id")) & (F.col("ch.order_id").isin([8, 38])), "left_anti")
)

with_latest = (
    eligible
    .alias("s")
    .join(df_payments.alias("c"), (F.col("c.order_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.customer_id",
        "s.order_date",
        "s.status",
        "s.amount",
        "s.shipping_fee",
        "s.promo_id",
        F.col("c.method"),
        F.col("c.paid_at").alias("payment_paid_at"),
    )
)

enriched = (
    with_latest
    .alias("a")
    .join(df_customers.alias("p"), F.col("a.customer_id") == F.col("p.id"), "left")
    .select(
        "a.id",
        "a.customer_id",
        "a.order_date",
        "a.promo_id",
        "a.method",
        F.col("p.signup_date"),
        F.col("p.name"),
        F.col("p.city"),
    )
)

with_latest_orders = (
    enriched
    .alias("s")
    .join(df_order_items.alias("c"), (F.col("c.order_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.unit_price"))
)

result = (
    with_latest_orders
    .alias("s")
    .join(df_customers.alias("p"), F.col("s.customer_id") == F.col("p.id"), "left")
    .groupingSets(
        [["country", "order_city"], ["country"], []],
        F.col("p.country").alias("country"),
        F.col("s.city").alias("order_city"),
    )
    .agg(
        F.count("*").alias("rows_total"),
        F.max(F.col("s.unit_price")).alias("max_unit_price"),
        F.min(F.col("s.promo_id")).alias("min_promo_id"),
        F.grouping_id().alias("level_id"),
    )
    .orderBy(F.col("rows_total").asc_nulls_last())
)
