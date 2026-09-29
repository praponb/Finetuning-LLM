from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status")
w_grp = Window.partitionBy("promo_code")
w_part = Window.partitionBy("status", "channel").orderBy("amount_with_shipping")
w4 = Window.partitionBy("channel", "status").orderBy(F.col("amount_with_shipping_2").desc_nulls_first())

cleaned = (
    df_orders
    .filter((F.col("order_date") >= F.to_date(F.lit("2023-03-04"))) & (F.col("channel").eqNullSafe(F.lit("store"))))
    .select(
        "id",
        "order_date",
        "status",
        "channel",
        "discount",
        "shipping_fee",
        (F.col("amount") + F.col("shipping_fee")).alias("amount_with_shipping"),
        (F.col("shipping_fee") - F.col("amount")).alias("amount_with_shipping_2"),
        F.coalesce("channel", F.lit("NONE")).alias("resolved_channel"),
    )
)

eligible = (
    cleaned
    .alias("s")
    .join(df_order_items.alias("x"), (F.col("x.order_id") == F.col("s.id")) & (F.col("x.product_id").isNotNull()), "left_anti")
)

kept_keys = (
    eligible
    .select("channel", "status")
    .intersect(
        df_orders
        .filter(F.col("shipping_fee").between(12, 37))
        .select("channel", "status")
    )
)

reconciled = (
    eligible
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.channel") == F.col("k.channel")) & (F.col("s.status") == F.col("k.status")), "inner")
    .select("s.*")
)

with_window = (
    reconciled
    .alias("a")
    .join(df_promotions.alias("pr"), (F.col("a.channel") == F.col("pr.channel")) & (F.col("a.order_date") >= F.col("pr.starts_on")) & (F.col("a.order_date") < F.col("pr.ends_on") + F.expr("INTERVAL 14 DAYS")), "left")
    .select(
        "a.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code"),
        F.col("pr.discount_pct"),
    )
)

result = (
    with_window
    .select(
        "id",
        "status",
        "channel",
        "discount",
        "amount_with_shipping_2",
        "promo_code",
        F.min("amount_with_shipping_2").over(w).alias("status_min_amount_with_shipping_2"),
        F.max("amount_with_shipping_2").over(w_grp).alias("promo_code_max_amount_with_shipping_2"),
        F.dense_rank().over(w_part).alias("amount_with_shipping_dense_rank"),
        F.percent_rank().over(w4).alias("amount_with_shipping_2_percent_rank"),
    )
)
