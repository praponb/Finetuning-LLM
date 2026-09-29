from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel", "promotion_promo_code")
w2 = Window.partitionBy("promo_code").orderBy("ends_on", "id", "promo_id").rowsBetween(-3, -1)
w3 = Window.partitionBy("channel").orderBy("discount_pct", "id", "promo_id")
w4 = Window.partitionBy("promotion_promo_code").orderBy(F.col("prev_discount_pct").desc(), "id", "promo_id")
w5 = Window.partitionBy("promo_code").orderBy("promo_id_share", "id", "promo_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("channel").orderBy("discount_pct_avg_3_prior", "id", "promo_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy("running_promotion_discount_pct_tier").orderBy(F.col("bundle_avg_event_seq").desc())

attributed = (
    df_promotions
    .alias("e")
    .join(df_promotions.alias("pr"), (F.col("e.channel") == F.col("pr.channel")) & (F.col("e.ends_on") >= F.col("pr.starts_on")) & (F.col("e.ends_on") < F.col("pr.ends_on") + F.expr("INTERVAL 30 DAYS")), "inner")
    .select(
        "e.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code").alias("promotion_promo_code"),
        F.col("pr.discount_pct").alias("promotion_discount_pct"),
    )
)

metrics = (
    attributed
    .select(
        "*",
        F.round((F.col("promo_id") / F.sum("promo_id").over(w)), 4).alias("promo_id_share"),
        F.avg("discount_pct").over(w2).alias("discount_pct_avg_3_prior"),
        F.lag("discount_pct", 2).over(w3).alias("prev_discount_pct"),
    )
)

enriched = (
    metrics
    .select(
        "*",
        F.row_number().over(w4).alias("event_seq"),
        F.sum("promotion_discount_pct").over(w5).alias("running_promotion_discount_pct"),
        F.sum("prev_discount_pct").over(w6).alias("running_prev_discount_pct"),
    )
)

result = (
    enriched
    .groupBy(
        F.when(F.col("running_promotion_discount_pct") >= 2, "LARGE").otherwise("SMALL").alias("running_promotion_discount_pct_tier"),
        F.when(F.col("prev_discount_pct") >= 0.36, "PREMIUM").otherwise("STANDARD").alias("prev_discount_pct_tier"),
        F.date_trunc("week", "starts_on").alias("week_start"),
    )
    .agg(
        F.avg(F.when(F.col("promotion_promo_code") == "BUNDLE", F.col("event_seq"))).alias("bundle_avg_event_seq"),
        F.count_if(F.col("promotion_promo_code") == "BUNDLE").alias("bundle_events"),
        F.percentile_approx("promo_id", [0.25, 0.5, 0.75]).alias("promo_id_quartiles"),
    )
    .filter(F.col("bundle_events") > 1)
    .withColumn("bundle_avg_event_seq_percent_rank", F.percent_rank().over(w7))
    .orderBy("bundle_avg_event_seq")
)
