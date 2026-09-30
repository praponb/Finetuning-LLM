from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.partitionBy("carrier").orderBy("cost", "id")
w_running = Window.partitionBy("carrier").orderBy("delivered_date", "id").rowsBetween(-5, Window.currentRow)
w3 = Window.partitionBy("channel", "order_id").orderBy(F.col("discount_pct").desc_nulls_first())
w4 = Window.orderBy(F.col("shipping_fee").desc_nulls_first())
w5 = Window.partitionBy("order_status")

cleaned = (
    df_shipments
    .filter(F.col("shipped_date").between("2023-12-11", "2025-07-01"))
    .select("*", F.add_months("delivered_date", 1).alias("delivered_date_plus_months"))
)

scored = (
    cleaned
    .withColumn("prev_cost", F.lag("cost").over(w_grp))
    .withColumn("next_cost", F.lead("cost").over(w_grp))
    .withColumn("event_seq", F.row_number().over(w_grp))
    .withColumn("cost_avg_5", F.avg("cost").over(w_running))
)

enriched = (
    scored
    .alias("e")
    .join(df_orders.alias("b"), F.col("e.order_id") == F.col("b.id"), "left")
    .select(
        "e.id",
        "e.order_id",
        "e.delivered_date",
        "e.prev_cost",
        "e.next_cost",
        F.col("b.channel"),
        F.col("b.shipping_fee"),
        F.col("b.status").alias("order_status"),
    )
)

kept_keys = (
    enriched
    .select("order_id", "id")
    .intersect(
        df_shipments
        .filter(F.col("shipped_date").between("2023-10-01", "2025-01-01"))
        .select("order_id", "id")
    )
)

kept_rows = (
    enriched
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.order_id") == F.col("k.order_id")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

with_window = (
    kept_rows
    .alias("s")
    .join(df_promotions.alias("pr"), (F.col("s.channel") == F.col("pr.channel")) & F.col("s.delivered_date").between(F.col("pr.starts_on"), F.col("pr.ends_on")), "left")
    .select(
        "s.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code"),
        F.col("pr.discount_pct"),
    )
)

with_window_shipments = (
    with_window
    .alias("e")
    .join(df_promotions.alias("pr"), (F.coalesce(F.col("e.channel"), F.lit("web")) == F.col("pr.channel")) & (F.col("e.delivered_date") >= F.col("pr.starts_on")) & (F.col("e.delivered_date") <= F.date_add(F.col("pr.ends_on"), 10)), "inner")
    .select(
        "e.*",
        F.col("pr.id").alias("promo_id_2"),
        F.col("pr.promo_code").alias("promotion_promo_code"),
        F.col("pr.discount_pct").alias("promotion_discount_pct"),
    )
)

scored_shipments = (
    with_window_shipments
    .filter(F.col("order_id") == 8)
    .select(
        "id",
        "order_id",
        "order_status",
        "promo_id",
        "promo_code",
        "discount_pct",
        "promo_id_2",
        "promotion_discount_pct",
        F.percent_rank().over(w3).alias("discount_pct_percent_rank"),
        F.rank().over(w4).alias("shipping_fee_rank"),
        F.round((F.col("promo_id") / F.sum("promo_id").over(w5)), 4).alias("promo_id_share"),
    )
)

result = (
    scored_shipments
    .groupBy("order_id", "promo_id")
    .agg(
        F.countDistinct("order_id").alias("distinct_order_ids"),
        F.count(F.when(F.col("order_status") == "COMPLETED", 1)).alias("completed_rows"),
        F.sum(F.when(F.col("order_status") == "RETURNED", F.col("promo_id_share"))).alias("returned_total_promo_id_share"),
        F.count("*").alias("num_rows"),
        F.percentile_approx("discount_pct", 0.95).alias("discount_pct_p95"),
        F.percentile_approx("promotion_discount_pct", 0.5).alias("promotion_discount_pct_p50"),
    )
)
