from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id"), F.col("s.promo_id")).orderBy(F.col("c.delivered_date").desc(), F.col("c.id").desc())

filtered = (
    df_orders
    .filter(F.col("amount").between(800, 870) & (F.col("order_date") >= F.lit("2023-03-14").cast("date")))
    .select(
        "id",
        "store_id",
        "order_date",
        "status",
        "channel",
        "amount",
        "discount",
        "shipping_fee",
        F.ceil("amount").alias("amount_whole"),
    )
)

matched = (
    filtered
    .alias("e")
    .join(df_promotions.alias("pr"), (F.col("e.channel") == F.col("pr.channel")) & (F.col("e.order_date") >= F.col("pr.starts_on")) & (F.col("e.order_date") < F.col("pr.ends_on") + F.expr("INTERVAL 30 DAYS")), "left")
    .select(
        "e.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code"),
        F.col("pr.discount_pct"),
    )
)

with_latest = (
    matched
    .alias("s")
    .join(df_shipments.alias("c"), (F.col("c.order_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.cost"), F.col("c.shipped_date"))
)

joined = (
    with_latest
    .alias("e")
    .join(df_stores.alias("d"), F.col("e.store_id") == F.col("d.id"), "left")
    .filter(F.col("d.sqft") > 11756)
    .select(
        "e.id",
        "e.store_id",
        "e.order_date",
        "e.discount",
        "e.amount_whole",
        "e.promo_id",
        "e.promo_code",
        "e.discount_pct",
        F.col("d.store_name"),
        F.col("d.region"),
        F.col("d.city"),
    )
)

enriched = (
    joined
    .alias("src")
    .join(df_stores.alias("p"), F.col("src.store_id") == F.col("p.id"), "left")
    .select(
        "src.id",
        "src.store_id",
        "src.amount_whole",
        "src.promo_id",
        "src.discount_pct",
        "src.store_name",
        "src.region",
        F.col("p.sqft"),
    )
)

result = (
    enriched
    .select("id", "store_id", "promo_id", "store_name", "region", "sqft")
    .orderBy(F.col("sqft").desc(), F.col("store_id").desc())
)
