from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("order_id").orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("promotion_promo_code").orderBy("promotion_discount_pct")
w4 = Window.partitionBy("promotion_promo_code", "promotion_promo_code_2").orderBy("amount", "id", "promo_id", "promo_id_2", "promo_id_3", "promo_id_4").rowsBetween(Window.unboundedPreceding, Window.currentRow)

enriched = (
    df_order_items
    .alias("a")
    .join(df_orders.alias("o"), F.col("a.order_id") == F.col("o.id"), "left")
    .select(
        "a.id",
        "a.order_id",
        "a.quantity",
        F.col("o.order_date"),
        F.col("o.amount"),
        F.col("o.channel"),
    )
)

scored = (
    enriched
    .filter(F.col("order_date") >= "2025-08-01")
    .select(
        "id",
        "quantity",
        "order_date",
        "amount",
        "channel",
        F.sum("amount").over(w).alias("running_amount"),
        F.first("amount", ignorenulls=True).over(w2).alias("first_known_amount"),
    )
)

matched = (
    scored
    .alias("e")
    .join(df_promotions.alias("pr"), (F.col("e.channel") == F.col("pr.channel")) & F.col("e.order_date").between(F.col("pr.starts_on"), F.col("pr.ends_on")), "inner")
    .select(
        "e.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code"),
        F.col("pr.discount_pct"),
    )
)

with_window = (
    matched
    .alias("s")
    .join(df_promotions.alias("pr"), (F.col("s.channel") == F.col("pr.channel")) & F.col("s.order_date").between(F.col("pr.starts_on"), F.col("pr.ends_on")), "left")
    .select(
        "s.*",
        F.col("pr.id").alias("promo_id_2"),
        F.col("pr.promo_code").alias("promotion_promo_code"),
        F.col("pr.discount_pct").alias("promotion_discount_pct"),
    )
)

with_window_order_items = (
    with_window
    .alias("a")
    .join(df_promotions.alias("pr"), (F.col("a.channel").eqNullSafe(F.col("pr.channel"))) & (F.col("a.order_date") >= F.col("pr.starts_on")) & (F.col("a.order_date") < F.col("pr.ends_on") + F.expr("INTERVAL 30 DAYS")), "left")
    .select(
        "a.*",
        F.col("pr.id").alias("promo_id_3"),
        F.col("pr.promo_code").alias("promotion_promo_code_2"),
        F.col("pr.discount_pct").alias("promotion_discount_pct_2"),
    )
)

cleaned = (
    with_window_order_items
    .filter(F.col("promo_code").isNotNull())
    .withColumn("channel_title", F.initcap(F.trim("channel")))
    .withColumn("promo_code_clean", F.coalesce("promo_code", F.lit("DIRECT")))
)

matched_order_items = (
    cleaned
    .alias("e")
    .join(df_promotions.alias("pr"), (F.col("e.channel") == F.col("pr.channel")) & F.col("e.order_date").between(F.col("pr.starts_on"), F.col("pr.ends_on")), "inner")
    .select(
        "e.*",
        F.col("pr.id").alias("promo_id_4"),
        F.col("pr.promo_code").alias("promotion_promo_code_3"),
        F.col("pr.discount_pct").alias("promotion_discount_pct_3"),
    )
)

result = (
    matched_order_items
    .select(
        "id",
        "order_date",
        "promo_id",
        "promo_id_2",
        "promo_id_3",
        "promotion_discount_pct_2",
        "channel_title",
        "promo_id_4",
        "promotion_promo_code_3",
        "promotion_discount_pct_3",
        F.percent_rank().over(w3).alias("promotion_discount_pct_percent_rank"),
        F.sum("promo_id").over(w4).alias("running_promo_id"),
    )
)
