from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel").orderBy("ends_on", "id")
w2 = Window.partitionBy("channel")
w3 = Window.partitionBy("channel", "promo_code").orderBy("row_seq")

metrics = (
    df_promotions
    .filter(F.col("discount_pct").between(0.22, 0.28))
    .select(
        "*",
        F.lead("discount_pct").over(w).alias("next_discount_pct"),
        F.row_number().over(w).alias("row_seq"),
    )
)

attributed = (
    metrics
    .alias("s")
    .join(df_promotions.alias("pr"), (F.col("s.channel").eqNullSafe(F.col("pr.channel"))) & (F.col("s.ends_on") >= F.col("pr.starts_on")) & (F.col("s.ends_on") < F.col("pr.ends_on") + F.expr("INTERVAL 14 DAYS")), "inner")
    .select(
        "s.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code").alias("promotion_promo_code"),
        F.col("pr.discount_pct").alias("promotion_discount_pct"),
    )
)

enriched = (
    attributed
    .select(
        "id",
        "starts_on",
        "discount_pct",
        "next_discount_pct",
        "row_seq",
        "promo_id",
        "promotion_discount_pct",
        F.min("row_seq").over(w2).alias("channel_min_row_seq"),
        F.cume_dist().over(w3).alias("row_seq_cume_dist"),
    )
)

filtered = (
    enriched
    .filter(F.col("row_seq") == 6)
    .select(
        "*",
        (F.col("channel_min_row_seq") - F.col("row_seq_cume_dist")).alias("channel_min_row_seq_minus_row_seq_cume_dist"),
    )
)

cleaned = (
    filtered
    .filter(F.col("promo_id").isNotNull())
    .select(
        "*",
        F.when(F.col("next_discount_pct") >= 0.28, "High").when(F.col("next_discount_pct") >= 0.12, "Medium").otherwise("Low").alias("next_discount_pct_band"),
    )
)

result = (
    cleaned
    .groupBy(F.date_trunc("month", "starts_on").alias("activity_month"), "promo_id")
    .agg(
        F.percentile_approx("promo_id", 0.95).alias("promo_id_p95"),
        F.percentile_approx("promotion_discount_pct", 0.9).alias("promotion_discount_pct_p90"),
        F.round(F.avg("promo_id"), 2).alias("avg_promo_id"),
        F.percentile_approx("promotion_discount_pct", 0.5).alias("promotion_discount_pct_p50"),
    )
    .orderBy(F.col("promotion_discount_pct_p50").asc_nulls_first())
)
