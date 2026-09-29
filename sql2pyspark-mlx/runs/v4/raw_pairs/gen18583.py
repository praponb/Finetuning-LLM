from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id"), F.col("s.promo_id")).orderBy(F.col("c.quantity").desc(), F.col("c.id").desc())

cleaned = (
    df_orders
    .filter(F.col("order_date") >= F.to_date(F.lit("2023-06-11")))
    .select(
        "id",
        "customer_id",
        "order_date",
        "status",
        "channel",
        "amount",
        "discount",
        F.when(F.col("status").isin(["SHIPPED", "COMPLETED"]), "Core").otherwise("Other").alias("status_group"),
    )
)

enriched = (
    cleaned
    .alias("src")
    .join(df_customers.alias("p"), F.col("src.customer_id") == F.col("p.id"), "inner")
    .select("src.*", F.col("p.city"), F.col("p.email"), F.col("p.segment"))
)

attributed = (
    enriched
    .alias("e")
    .join(df_promotions.alias("pr"), (F.col("e.channel") == F.col("pr.channel")) & F.col("e.order_date").between(F.col("pr.starts_on"), F.col("pr.ends_on")), "left")
    .select(
        "e.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code"),
        F.col("pr.discount_pct"),
    )
)

with_last = (
    attributed
    .alias("s")
    .join(df_order_items.alias("c"), (F.col("c.order_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.unit_price"),
        F.col("c.quantity").alias("order_item_quantity"),
    )
)

result = (
    with_last
    .select(
        "id",
        "customer_id",
        "status",
        "channel",
        "amount",
        "discount",
        "status_group",
        "segment",
        "promo_id",
        F.when(F.col("status").rlike("^(SHIPPED|COMPLETED)$"), "GROUP_B").otherwise("PRIORITY").alias("status_zone"),
    )
    .orderBy(F.col("discount").desc())
)
