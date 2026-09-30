from pyspark.sql import functions as F

base = (
    df_promotions
    .filter((F.col("starts_on") > F.lit("2023-12-02").cast("date")) & F.col("channel").isNotNull())
    .select(
        "id",
        "channel",
        "promo_code",
        "starts_on",
        "discount_pct",
        F.length("promo_code").alias("promo_code_length"),
        F.coalesce("channel", F.lit("DIRECT")).alias("resolved_channel"),
    )
)

result = (
    base
    .alias("s")
    .join(df_promotions.alias("pr"), (F.coalesce(F.col("s.channel"), F.lit("store")) == F.col("pr.channel")) & F.col("s.starts_on").between(F.col("pr.starts_on"), F.col("pr.ends_on")), "inner")
    .select(
        "s.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code").alias("promotion_promo_code"),
        F.col("pr.discount_pct").alias("promotion_discount_pct"),
    )
)
