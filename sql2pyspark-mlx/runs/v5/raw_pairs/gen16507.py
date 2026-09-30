from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo_code").orderBy(F.col("starts_on").desc(), "id")
w2 = Window.partitionBy("promo_code", "channel").orderBy(F.col("promotion_discount_pct").desc(), "id", "promo_id")
w3 = Window.partitionBy("promo_code", "channel").orderBy("dedup_rank", "id", "promo_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.orderBy("running_avg_promo_id", "id", "promo_id")
w5 = Window.orderBy("promo_id", "id", "promo_id").rowsBetween(-3, -1)
w6 = Window.orderBy(F.col("promo_id").desc(), "id", "promo_id")

ranked = (
    df_promotions
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
    .select("id", "channel", "ends_on", "discount_pct")
)

with_window = (
    ranked
    .alias("s")
    .join(df_promotions.alias("pr"), (F.col("s.channel").eqNullSafe(F.col("pr.channel"))) & (F.col("s.ends_on") >= F.col("pr.starts_on")) & (F.col("s.ends_on") < F.col("pr.ends_on") + F.expr("INTERVAL 7 DAYS")), "left")
    .select(
        "s.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code"),
        F.col("pr.discount_pct").alias("promotion_discount_pct"),
    )
)

ranked_promotions = (
    with_window
    .withColumn("dedup_rank", F.row_number().over(w2))
    .filter(F.col("dedup_rank") == 1)
)

base = (
    ranked_promotions
    .filter((F.col("ends_on") >= F.to_date(F.lit("2023-11-03"))) & F.col("discount_pct").between(0.13, 0.24))
    .select(
        "id",
        "channel",
        "promo_id",
        "promo_code",
        "promotion_discount_pct",
        "dedup_rank",
        F.trim("promo_code").alias("clean_promo_code"),
    )
)

latest = (
    base
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
)

enriched = (
    latest
    .select(
        "id",
        "promo_id",
        "promo_code",
        "clean_promo_code",
        "rn",
        F.min("promo_id").over(w3).alias("running_min_promo_id"),
        F.sum("promo_id").over(w3).alias("running_sum_promo_id"),
        F.avg("promo_id").over(w3).alias("running_avg_promo_id"),
    )
)

with_windows = (
    enriched
    .select(
        "id",
        "promo_id",
        "clean_promo_code",
        "running_min_promo_id",
        "running_sum_promo_id",
        "running_avg_promo_id",
        F.lag("running_sum_promo_id", 1).over(w4).alias("prev_running_sum_promo_id"),
        F.avg("promo_id").over(w5).alias("promo_id_avg_3_prior"),
        F.ntile(4).over(w6).alias("promo_id_bucket"),
    )
)

result = (
    with_windows
    .groupBy("promo_id")
    .agg(
        F.percentile_approx("prev_running_sum_promo_id", [0.25, 0.5, 0.75]).alias("prev_running_sum_promo_id_quartiles"),
        F.percentile_approx("promo_id", 0.5).alias("promo_id_p50"),
        F.count("*").alias("cnt"),
    )
    .orderBy(F.col("cnt").desc())
)
