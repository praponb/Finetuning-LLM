from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel", "promo_code").orderBy("starts_on", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_hist = Window.partitionBy("promo_code").orderBy("ends_on", "id").rowsBetween(-5, Window.currentRow)
w_ordered = Window.partitionBy("channel", "promo_code").orderBy("discount_pct", "id", "promo_id").rowsBetween(-6, Window.currentRow)
w4 = Window.partitionBy("promo_code").orderBy("discount_pct")
w5 = Window.partitionBy("channel").orderBy(F.col("last7_count_promo_id").desc(), "id", "promo_id")
w6 = Window.partitionBy("promotion_promo_code").orderBy("last7_count_promo_id", "id", "promo_id")
w7 = Window.partitionBy("promo_code", "promotion_promo_code").orderBy("ends_on", "id", "promo_id").rowsBetween(-3, Window.currentRow)

enriched = (
    df_promotions
    .filter(F.col("channel") != "partner")
    .select(
        "id",
        "channel",
        "promo_code",
        "starts_on",
        "ends_on",
        "discount_pct",
        F.first("channel", ignorenulls=True).over(w).alias("first_known_channel"),
        F.avg("discount_pct").over(w_hist).alias("discount_pct_avg_5"),
    )
)

with_window = (
    enriched
    .alias("s")
    .join(df_promotions.alias("pr"), (F.col("s.channel").eqNullSafe(F.col("pr.channel"))) & (F.col("s.ends_on") >= F.col("pr.starts_on")) & (F.col("s.ends_on") < F.col("pr.ends_on") + F.expr("INTERVAL 14 DAYS")), "left")
    .select(
        "s.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code").alias("promotion_promo_code"),
        F.col("pr.discount_pct").alias("promotion_discount_pct"),
    )
)

scored = (
    with_window
    .select(
        "id",
        "channel",
        "promo_code",
        "ends_on",
        "discount_pct",
        "first_known_channel",
        "promo_id",
        "promotion_promo_code",
        F.count("promo_id").over(w_ordered).alias("last7_count_promo_id"),
        F.max("promo_id").over(w_ordered).alias("last7_max_promo_id"),
        F.avg("promo_id").over(w_ordered).alias("last7_avg_promo_id"),
    )
)

metrics = (
    scored
    .select(
        "id",
        "promo_code",
        "ends_on",
        "discount_pct",
        "promo_id",
        "promotion_promo_code",
        "last7_count_promo_id",
        "last7_max_promo_id",
        "last7_avg_promo_id",
        F.cume_dist().over(w4).alias("discount_pct_cume_dist"),
        F.row_number().over(w5).alias("row_seq"),
        F.lead("last7_count_promo_id", 2).over(w6).alias("next_last7_count_promo_id"),
    )
)

enriched_promotions = (
    metrics
    .select(
        "id",
        "promo_id",
        "promotion_promo_code",
        "last7_avg_promo_id",
        "row_seq",
        F.avg("last7_avg_promo_id").over(w7).alias("last4_avg_last7_avg_promo_id"),
        F.min("last7_avg_promo_id").over(w7).alias("last4_min_last7_avg_promo_id"),
        F.count("last7_avg_promo_id").over(w7).alias("last4_count_last7_avg_promo_id"),
        F.max("last7_avg_promo_id").over(w7).alias("last4_max_last7_avg_promo_id"),
    )
)

result = (
    enriched_promotions
    .groupBy("promo_id")
    .agg(
        F.sum(F.when(F.col("promotion_promo_code") == "BUNDLE", F.col("last4_avg_last7_avg_promo_id"))).alias("bundle_total_last4_avg_last7_avg_promo_id"),
        F.countDistinct("promotion_promo_code").alias("distinct_promotion_promo_codes"),
        F.sum(F.when(F.col("promotion_promo_code") == "VIP", F.col("last4_avg_last7_avg_promo_id")).otherwise(0)).alias("vip_last4_avg_last7_avg_promo_id"),
        F.countDistinct("promotion_promo_code").alias("distinct_promotion_promo_codes_2"),
        F.percentile_approx("last7_avg_promo_id", [0.25, 0.5, 0.75]).alias("last7_avg_promo_id_quartiles"),
        F.round(F.avg("promo_id"), 2).alias("avg_promo_id"),
    )
    .filter(F.col("distinct_promotion_promo_codes_2") > 1)
    .orderBy(F.col("distinct_promotion_promo_codes_2").desc_nulls_first())
)
