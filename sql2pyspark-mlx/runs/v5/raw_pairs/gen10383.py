from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel").orderBy("order_date", "id").rowsBetween(-5, Window.currentRow)
w2 = Window.partitionBy("customer_id")
w3 = Window.partitionBy("status").orderBy("discount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_recent = Window.partitionBy("store_id").orderBy(F.col("discount").desc_nulls_first())
w5 = Window.partitionBy("customer_id").orderBy(F.col("promo_id").desc())
w_running = Window.partitionBy("promo_code").orderBy("promo_id_2", "id", "promo_id", "promo_id_2")
w7 = Window.partitionBy("customer_id").orderBy("promotion_discount_pct", "id", "promo_id", "promo_id_2")
w8 = Window.partitionBy(F.col("s.id"), F.col("s.promo_id"), F.col("s.promo_id_2")).orderBy(F.col("c.unit_price").desc(), F.col("c.id").desc())

scored = (
    df_orders
    .select(
        "id",
        "customer_id",
        "store_id",
        "order_date",
        "channel",
        "discount",
        F.avg("shipping_fee").over(w).alias("shipping_fee_avg_5"),
        F.corr("discount", "shipping_fee").over(w2).alias("corr_discount_shipping_fee"),
        F.last("shipping_fee", ignorenulls=True).over(w3).alias("last_known_shipping_fee"),
        F.cume_dist().over(w_recent).alias("discount_cume_dist"),
    )
)

attributed = (
    scored
    .alias("a")
    .join(df_promotions.alias("pr"), (F.coalesce(F.col("a.channel"), F.lit("store")) == F.col("pr.channel")) & (F.col("a.order_date") >= F.col("pr.starts_on")) & (F.col("a.order_date") < F.col("pr.ends_on") + F.expr("INTERVAL 30 DAYS")), "left")
    .select(
        "a.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code"),
        F.col("pr.discount_pct"),
    )
)

enriched = (
    attributed
    .alias("a")
    .join(df_customers.alias("b"), F.col("a.customer_id").eqNullSafe(F.col("b.id")), "left")
    .select("a.*", F.col("b.age"), F.col("b.signup_date"))
)

deduped = (
    enriched
    .withColumn("dedup_rank", F.rank().over(w5))
    .filter(F.col("dedup_rank") == 1)
    .drop("dedup_rank")
)

matched = (
    deduped
    .alias("a")
    .join(df_promotions.alias("pr"), (F.col("a.channel") == F.col("pr.channel")) & (F.col("a.signup_date") >= F.col("pr.starts_on")) & (F.col("a.signup_date") <= F.date_add(F.col("pr.ends_on"), 10)), "left")
    .select(
        "a.*",
        F.col("pr.id").alias("promo_id_2"),
        F.col("pr.promo_code").alias("promotion_promo_code"),
        F.col("pr.discount_pct").alias("promotion_discount_pct"),
    )
)

with_windows = (
    matched
    .select(
        "*",
        F.lead("promo_id").over(w_running).alias("next_promo_id"),
        F.lag("promo_id").over(w_running).alias("prev_promo_id"),
        F.lag("corr_discount_shipping_fee").over(w_running).alias("prev_corr_discount_shipping_fee"),
        F.row_number().over(w_running).alias("event_seq"),
        F.lag("discount", 1).over(w7).alias("prev_discount"),
    )
)

latest_match = (
    with_windows
    .alias("s")
    .join(df_order_items.alias("c"), (F.col("c.order_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w8))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.quantity"))
)

stats = (
    latest_match
    .groupBy(F.when(F.col("age") >= 63, "HIGH").otherwise("LOW").alias("age_tier"))
    .agg(
        F.round(F.sum("discount_cume_dist") / F.nullif(F.count(F.when(F.col("promotion_promo_code") == "VIP", 1)), F.lit(0)), 2).alias("discount_cume_dist_per_vip"),
        F.count("*").alias("event_count"),
    )
)

result = (
    stats
    .select("age_tier")
    .orderBy(F.col("age_tier").desc_nulls_first())
)
