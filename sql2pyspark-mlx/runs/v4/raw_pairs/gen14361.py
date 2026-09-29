from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promotion_promo_code").orderBy("starts_on", "id", "promo_id")

filtered = (
    df_promotions
    .filter((F.col("starts_on") > F.lit("2023-05-12").cast("date")) & F.col("channel").isNotNull())
    .select(
        "id",
        "channel",
        "promo_code",
        "starts_on",
        "discount_pct",
        F.date_format("starts_on", "yyyy-MM-dd").alias("starts_on_label"),
        F.substring("promo_code", 1, 2).alias("promo_code_prefix"),
    )
)

attributed = (
    filtered
    .alias("s")
    .join(df_promotions.alias("pr"), (F.coalesce(F.col("s.channel"), F.lit("web")) == F.col("pr.channel")) & (F.col("s.starts_on") >= F.col("pr.starts_on")) & (F.col("s.starts_on") < F.col("pr.ends_on") + F.expr("INTERVAL 14 DAYS")), "left")
    .select(
        "s.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code").alias("promotion_promo_code"),
        F.col("pr.discount_pct").alias("promotion_discount_pct"),
    )
)

deduped = (
    attributed
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
    .drop("rank_in_group")
)

result = (
    deduped
    .select(
        "id",
        "promo_id",
        "channel",
        F.col("discount_pct").alias("DISCOUNT_PCT_VAL"),
        F.col("promotion_discount_pct").alias("PROMOTION_DISCOUNT_PCT"),
    )
    .unpivot(
        ["id", "promo_id", "channel"],
        ["DISCOUNT_PCT_VAL", "PROMOTION_DISCOUNT_PCT"],
        "metric",
        "val",
    )
    .filter(F.col("val").isNotNull())
)
