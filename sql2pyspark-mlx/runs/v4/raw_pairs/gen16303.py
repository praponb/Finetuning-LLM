from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("is_partner").desc(), "id", "promo_id")
w2 = Window.partitionBy("promo_code").orderBy("amount", "id", "promo_id")
w3 = Window.orderBy("shipping_fee", "id", "promo_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("promo_code", "channel").orderBy("is_partner", "id", "promo_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

not_flagged = (
    df_orders
    .alias("s")
    .join(df_shipments.alias("c"), (F.col("c.order_id") == F.col("s.id")) & (F.col("c.delivered_date") <= F.col("s.order_date")), "left_anti")
)

not_flagged_orders = (
    not_flagged
    .alias("s")
    .join(df_shipments.alias("ch"), (F.col("ch.order_id") == F.col("s.id")) & (F.col("ch.shipped_date") < "2024-10-20"), "left_anti")
)

with_window = (
    not_flagged_orders
    .alias("e")
    .join(df_promotions.alias("pr"), (F.col("e.channel") == F.col("pr.channel")) & (F.col("e.order_date") >= F.col("pr.starts_on")) & (F.col("e.order_date") < F.col("pr.ends_on") + F.expr("INTERVAL 30 DAYS")), "left")
    .select(
        "e.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code"),
        F.col("pr.discount_pct"),
    )
)

filtered = (
    with_window
    .filter(F.col("discount").between(0.15, 0.34))
    .select(
        "*",
        F.when(F.col("channel") == "partner", 1).otherwise(0).alias("is_partner"),
        F.coalesce("promo_code", F.lit("OTHER")).alias("resolved_promo_code"),
    )
)

scored = (
    filtered
    .withColumn("is_partner_bucket", F.ntile(5).over(w))
    .withColumn("next_discount", F.lead("discount", 1, 0).over(w2))
)

metrics = (
    scored
    .select(
        "*",
        F.sum("discount").over(w3).alias("cumulative_discount"),
        F.first("discount_pct", ignorenulls=True).over(w3).alias("first_known_discount_pct"),
        F.sum("shipping_fee").over(w4).alias("running_shipping_fee"),
    )
)

result = (
    metrics
    .select("id", "promo_id", "store_id", "discount_pct", "next_discount")
    .unpivot(
        ["id", "promo_id", "store_id"],
        ["discount_pct", "next_discount"],
        "metric",
        "metric_value",
    )
    .filter(F.col("metric_value").isNotNull())
)
