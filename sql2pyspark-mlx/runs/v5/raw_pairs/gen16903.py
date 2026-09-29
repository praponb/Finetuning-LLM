from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo_id_tier").orderBy(F.col("avg_promo_id_2").desc())

base = (
    df_promotions
    .filter(F.col("ends_on") >= F.lit("2023-08-25").cast("date"))
    .select(
        "id",
        "channel",
        "promo_code",
        "starts_on",
        "ends_on",
        F.upper("promo_code").alias("promo_code_upper"),
        F.coalesce("channel", F.lit("NONE")).alias("channel_filled"),
    )
)

attributed = (
    base
    .alias("a")
    .join(df_promotions.alias("pr"), (F.col("a.channel").eqNullSafe(F.col("pr.channel"))) & (F.col("a.ends_on") >= F.col("pr.starts_on")) & (F.col("a.ends_on") < F.col("pr.ends_on") + F.expr("INTERVAL 30 DAYS")), "inner")
    .select(
        "a.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code").alias("promotion_promo_code"),
        F.col("pr.discount_pct"),
    )
)

filtered = (
    attributed
    .filter((F.col("ends_on") > F.lit("2023-09-21").cast("date")) & F.col("promotion_promo_code").isNotNull())
    .select(
        "id",
        "channel",
        "promo_code",
        "starts_on",
        "ends_on",
        "promo_id",
        "promotion_promo_code",
        (F.col("discount_pct") - F.col("promo_id")).alias("discount_pct_minus_promo_id"),
    )
)

attributed_promotions = (
    filtered
    .alias("e")
    .join(df_promotions.alias("pr"), (F.col("e.channel") == F.col("pr.channel")) & (F.col("e.ends_on") >= F.col("pr.starts_on")) & (F.col("e.ends_on") < F.col("pr.ends_on") + F.expr("INTERVAL 7 DAYS")), "left")
    .select(
        "e.*",
        F.col("pr.id").alias("promo_id_2"),
        F.col("pr.promo_code").alias("promotion_promo_code_2"),
        F.col("pr.discount_pct"),
    )
)

cleaned = (
    attributed_promotions
    .filter(F.col("ends_on") > F.to_date(F.lit("2023-08-08")))
    .select(
        "id",
        "channel",
        "promo_code",
        "starts_on",
        "promo_id",
        "promotion_promo_code",
        "discount_pct_minus_promo_id",
        "promo_id_2",
        "discount_pct",
        F.round("discount_pct", 1).alias("discount_pct_rounded"),
        F.col("promo_id").cast("double").alias("promo_id_num"),
    )
)

result = (
    cleaned
    .groupBy(
        F.when(F.col("promo_id") >= 9, "HIGH").otherwise("LOW").alias("promo_id_tier"),
        "promotion_promo_code",
    )
    .agg(
        F.percentile_approx("promo_id_2", 0.5).alias("promo_id_2_p50"),
        F.count(F.when(F.col("channel") == "mobile", 1)).alias("mobile_count"),
        F.round(F.sum("discount_pct_minus_promo_id") / F.nullif(F.count(F.when(F.col("promo_code") == "SPRING10", 1)), F.lit(0)), 2).alias("discount_pct_minus_promo_id_per_spring10"),
        F.avg("promo_id").alias("avg_promo_id"),
        F.round(F.avg("promo_id_2"), 2).alias("avg_promo_id_2"),
    )
    .withColumn("avg_promo_id_2_dense_rank", F.dense_rank().over(w))
    .orderBy(F.col("avg_promo_id_2").desc_nulls_first())
)
