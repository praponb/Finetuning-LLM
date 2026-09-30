from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id", "promo_code").orderBy(F.col("order_date").desc(), "id", "promo_id")
w2 = Window.partitionBy(F.col("s.id"), F.col("s.promo_id")).orderBy(F.col("c.quantity").desc(), F.col("c.id").desc())

base = (
    df_orders
    .filter(F.col("channel") != "web")
    .withColumn("days_since_order_date", F.datediff(F.current_date(), "order_date"))
    .withColumn("order_date_plus_months", F.add_months("order_date", 6))
    .withColumn("channel_filled", F.coalesce("channel", F.lit("UNKNOWN")))
)

matched = (
    base
    .alias("s")
    .join(df_promotions.alias("pr"), (F.coalesce(F.col("s.channel"), F.lit("store")) == F.col("pr.channel")) & (F.col("s.order_date_plus_months") >= F.col("pr.starts_on")) & (F.col("s.order_date_plus_months") < F.col("pr.ends_on") + F.expr("INTERVAL 7 DAYS")), "inner")
    .select(
        "s.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code"),
        F.col("pr.discount_pct"),
    )
)

deduped = (
    matched
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
    .select(
        "id",
        "store_id",
        "order_date",
        "status",
        "channel",
        "discount",
        "shipping_fee",
        "days_since_order_date",
        "order_date_plus_months",
        "promo_id",
        "discount_pct",
    )
)

latest_match = (
    deduped
    .alias("s")
    .join(df_order_items.alias("c"), (F.col("c.order_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.store_id",
        "s.channel",
        "s.discount",
        "s.shipping_fee",
        "s.days_since_order_date",
        "s.order_date_plus_months",
        "s.promo_id",
        F.col("c.unit_price").alias("order_item_unit_price"),
        F.col("c.quantity"),
    )
)

result = (
    latest_match
    .select(
        "id",
        "promo_id",
        "channel",
        "order_item_unit_price",
        "discount",
        "shipping_fee",
    )
    .unpivot(
        ids=["id", "promo_id", "channel"],
        values=["order_item_unit_price", "discount", "shipping_fee"],
        variableColumnName="metric",
        valueColumnName="reading",
    )
    .filter(F.col("reading").isNotNull())
)
