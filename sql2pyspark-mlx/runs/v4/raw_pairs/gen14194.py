from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_status").orderBy(F.col("shipped_date").asc_nulls_last(), "id")

joined = (
    df_shipments
    .alias("src")
    .join(df_orders.alias("b"), F.col("src.order_id") == F.col("b.id"), "left")
    .select(
        "src.*",
        F.col("b.status").alias("order_status"),
        F.col("b.discount"),
        F.col("b.shipping_fee"),
    )
)

joined_shipments = (
    joined
    .alias("src")
    .join(df_orders.alias("d"), F.col("src.order_id") == F.col("d.id"), "left")
    .select(
        "src.*",
        F.col("d.channel"),
        F.col("d.shipping_fee").alias("order_shipping_fee"),
        F.col("d.amount"),
    )
)

latest = (
    joined_shipments
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
    .drop("rank_in_group")
)

matched = (
    latest
    .alias("s")
    .join(df_promotions.alias("pr"), (F.col("s.channel") == F.col("pr.channel")) & F.col("s.shipped_date").between(F.col("pr.starts_on"), F.col("pr.ends_on")), "left")
    .select(
        "s.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code"),
        F.col("pr.discount_pct"),
    )
)

joined_shipments_2 = (
    matched
    .alias("s")
    .join(df_orders.alias("b"), F.col("s.order_id") == F.col("b.id"), "inner")
    .select(
        "s.*",
        F.col("b.discount").alias("order_discount"),
        F.col("b.shipping_fee").alias("order_shipping_fee_2"),
    )
)

result = (
    joined_shipments_2
    .select("id", "promo_id", "promo_code", "discount_pct", "order_discount", "cost")
    .unpivot(
        ["id", "promo_id", "promo_code"],
        ["discount_pct", "order_discount", "cost"],
        "metric_name",
        "value",
    )
    .filter(F.col("value").isNotNull())
)
