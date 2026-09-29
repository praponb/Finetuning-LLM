from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo_code").orderBy("ends_on_plus_65d", "id", "promo_id")
w2 = Window.partitionBy("promotion_promo_code_2", "promotion_promo_code").orderBy("promo_id_2", "id", "promo_id", "promo_id_2")
w3 = Window.partitionBy("promo_code", "promotion_promo_code").orderBy(F.col("promo_id_2").desc(), "id", "promo_id", "promo_id_2")
w4 = Window.partitionBy("channel").orderBy(F.col("ends_on_plus_65d").desc_nulls_last(), "id", "promo_id", "promo_id_2")

valid = (
    df_promotions
    .filter((F.col("starts_on") > F.lit("2023-08-01").cast("date")) & F.col("channel").isin(["store", "partner"]))
    .select(
        "id",
        "channel",
        "promo_code",
        "ends_on",
        "discount_pct",
        F.date_add("ends_on", 65).alias("ends_on_plus_65d"),
    )
)

attributed = (
    valid
    .alias("s")
    .join(df_promotions.alias("pr"), (F.coalesce(F.col("s.channel"), F.lit("web")) == F.col("pr.channel")) & (F.col("s.ends_on_plus_65d") >= F.col("pr.starts_on")) & (F.col("s.ends_on_plus_65d") < F.col("pr.ends_on") + F.expr("INTERVAL 14 DAYS")), "left")
    .select(
        "s.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code").alias("promotion_promo_code"),
        F.col("pr.discount_pct").alias("promotion_discount_pct"),
    )
)

deduped = (
    attributed
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
    .select(
        "id",
        "channel",
        "promo_code",
        "ends_on_plus_65d",
        "promo_id",
        "promotion_promo_code",
        "promotion_discount_pct",
    )
)

matched = (
    deduped
    .alias("a")
    .join(df_promotions.alias("pr"), (F.col("a.channel").eqNullSafe(F.col("pr.channel"))) & (F.col("a.ends_on_plus_65d") >= F.col("pr.starts_on")) & (F.col("a.ends_on_plus_65d") <= F.date_add(F.col("pr.ends_on"), 10)), "left")
    .select(
        "a.*",
        F.col("pr.id").alias("promo_id_2"),
        F.col("pr.promo_code").alias("promotion_promo_code_2"),
        F.col("pr.discount_pct"),
    )
)

metrics = (
    matched
    .select(
        "*",
        F.lag("discount_pct").over(w2).alias("prev_discount_pct"),
        F.lag("promo_id").over(w2).alias("prev_promo_id"),
        F.lead("promo_id").over(w2).alias("next_promo_id"),
        F.row_number().over(w2).alias("seq_no"),
        F.row_number().over(w3).alias("seq_no_2"),
    )
)

deduped_promotions = (
    metrics
    .withColumn("row_num", F.row_number().over(w4))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

result = (
    deduped_promotions
    .groupBy(F.date_trunc("MONTH", "ends_on_plus_65d").alias("activity_month"))
    .agg(
        F.round(F.sum("discount_pct") / F.nullif(F.count(F.when(F.col("channel") == "web", 1)), F.lit(0)), 2).alias("discount_pct_per_web"),
        F.size(F.collect_set("promo_code")).alias("num_promo_codes"),
        F.percentile_approx("next_promo_id", 0.5).alias("next_promo_id_p50"),
        F.sum(F.when(F.col("promotion_promo_code") == "BUNDLE", F.col("prev_discount_pct")).otherwise(0)).alias("bundle_prev_discount_pct"),
    )
)
