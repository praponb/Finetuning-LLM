from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel").orderBy(F.col("discount_pct").desc(), "id")
w2 = Window.partitionBy("channel", "promo_code").orderBy(F.col("promo_id").desc(), "id", "promo_id")
w3 = Window.partitionBy("promotion_promo_code")

base = (
    df_promotions
    .filter(F.col("channel").isNotNull() & (F.col("ends_on") > "2023-06-01"))
    .select(
        "*",
        F.concat("promo_code", F.lit(" "), "channel").alias("promo_code_channel"),
        F.coalesce("channel", F.lit("OTHER")).alias("channel_filled"),
    )
)

deduped = (
    base
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
)

attributed = (
    deduped
    .alias("a")
    .join(df_promotions.alias("pr"), (F.col("a.channel").eqNullSafe(F.col("pr.channel"))) & F.col("a.starts_on").between(F.col("pr.starts_on"), F.col("pr.ends_on")), "inner")
    .select(
        "a.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code").alias("promotion_promo_code"),
        F.col("pr.discount_pct").alias("promotion_discount_pct"),
    )
)

with_windows = (
    attributed
    .withColumn("row_seq", F.row_number().over(w2))
    .withColumn("discount_pct_share", F.round((F.col("discount_pct") / F.sum("discount_pct").over(w3)), 4))
)

result = (
    with_windows
    .groupBy(
        F.date_trunc("YEAR", "starts_on").alias("year_start"),
        F.when(F.col("row_seq") >= 8, "LARGE").otherwise("SMALL").alias("row_seq_tier"),
    )
    .agg(
        F.max("promo_id").alias("max_promo_id"),
        F.percentile_approx("promo_id", [0.25, 0.5, 0.75]).alias("promo_id_quartiles"),
        F.countDistinct("promo_code").alias("distinct_promo_codes"),
    )
    .orderBy(F.col("distinct_promo_codes").desc())
)
