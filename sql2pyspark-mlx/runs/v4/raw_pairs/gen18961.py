from pyspark.sql import functions as F

remaining_keys = (
    df_promotions
    .select("channel", "id")
    .intersectAll(
        df_promotions
        .filter(F.col("discount_pct") > 0.2)
        .select("channel", "id")
    )
)

reconciled = (
    df_promotions
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.channel") == F.col("k.channel")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .select("id", "channel", "promo_code", "starts_on")
    .orderBy(F.col("starts_on").desc(), F.col("channel").desc())
)
