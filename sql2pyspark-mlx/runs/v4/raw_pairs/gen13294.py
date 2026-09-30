from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy(F.col("delivered_date").desc(), "id")

joined = (
    df_shipments
    .alias("src")
    .join(df_orders.alias("b"), F.col("src.order_id") == F.col("b.id"), "inner")
    .select("src.*", F.col("b.channel"))
)

deduped = (
    joined
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
    .drop("rank_in_group")
)

with_window = (
    deduped
    .alias("s")
    .join(df_promotions.alias("pr"), (F.col("s.channel").eqNullSafe(F.col("pr.channel"))) & (F.col("s.shipped_date") >= F.col("pr.starts_on")) & (F.col("s.shipped_date") < F.col("pr.ends_on") + F.expr("INTERVAL 14 DAYS")), "left")
    .select(
        "s.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code"),
        F.col("pr.discount_pct"),
    )
)

with_window_shipments = (
    with_window
    .alias("s")
    .join(df_promotions.alias("pr"), (F.col("s.channel").eqNullSafe(F.col("pr.channel"))) & F.col("s.shipped_date").between(F.col("pr.starts_on"), F.col("pr.ends_on")), "inner")
    .select(
        "s.*",
        F.col("pr.id").alias("promo_id_2"),
        F.col("pr.promo_code").alias("promotion_promo_code"),
        F.col("pr.discount_pct").alias("promotion_discount_pct"),
    )
)

result = (
    with_window_shipments
    .cube(
        F.when(F.col("promo_id_2") >= 11, "HIGH").otherwise("LOW").alias("promo_id_2_tier"),
    )
    .agg(
        F.percentile_approx("promotion_discount_pct", [0.25, 0.5, 0.75]).alias("promotion_discount_pct_quartiles"),
        F.count(F.when(F.col("channel") == "mobile", 1)).alias("mobile_count"),
        F.round(F.sum("promotion_discount_pct") / F.nullif(F.count(F.when(F.col("status") == "DELIVERED", 1)), F.lit(0)), 2).alias("promotion_discount_pct_per_delivered"),
        F.sum(F.when(F.col("promo_code") == "SPRING10", F.col("promotion_discount_pct"))).alias("spring10_total_promotion_discount_pct"),
    )
    .orderBy(F.col("spring10_total_promotion_discount_pct").asc_nulls_last())
)
