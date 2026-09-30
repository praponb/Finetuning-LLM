from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status")
w2 = Window.orderBy(F.col("discount_pct").asc_nulls_last())

enriched = (
    df_payments
    .alias("e")
    .join(df_orders.alias("d"), F.col("e.order_id") == F.col("d.id"), "inner")
    .filter(F.col("d.amount") <= 780)
    .select("e.*", F.col("d.shipping_fee"), F.col("d.channel"), F.col("d.order_date"))
)

matched_keys = (
    enriched
    .select("status")
    .intersectAll(
        df_payments
        .filter(F.col("order_id").isNotNull())
        .select("status")
    )
)

verified = (
    enriched
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.status") == F.col("k.status")), "inner")
    .select("s.*")
)

valid = (
    verified
    .filter(F.col("channel").isNotNull() & (~F.col("channel").eqNullSafe(F.lit("web"))))
    .withColumn("shipping_fee_level", F.when(F.col("shipping_fee") >= 38, "High").when(F.col("shipping_fee") >= 8, "Medium").otherwise("Low"))
)

matched = (
    valid
    .alias("e")
    .join(df_promotions.alias("pr"), (F.col("e.channel") == F.col("pr.channel")) & (F.col("e.paid_at") >= F.col("pr.starts_on")) & (F.col("e.paid_at") <= F.date_add(F.col("pr.ends_on"), 3)), "left")
    .select(
        "e.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code"),
        F.col("pr.discount_pct"),
    )
)

enriched_payments = (
    matched
    .select(
        "*",
        F.max("shipping_fee").over(w).alias("status_max_shipping_fee"),
        F.cume_dist().over(w2).alias("discount_pct_cume_dist"),
    )
)

result = (
    enriched_payments
    .select(
        "id",
        "method",
        "status",
        "shipping_fee",
        "discount_pct",
        "discount_pct_cume_dist",
    )
    .orderBy(F.col("discount_pct_cume_dist").desc())
)
