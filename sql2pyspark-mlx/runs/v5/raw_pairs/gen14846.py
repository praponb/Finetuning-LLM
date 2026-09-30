from pyspark.sql import functions as F

remaining_keys = (
    df_promotions
    .select("channel", "id")
    .exceptAll(
        df_promotions
        .filter(F.col("starts_on").between("2023-05-01", "2024-05-01"))
        .select("channel", "id")
    )
)

reconciled = (
    df_promotions
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.channel") == F.col("k.channel")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

base = (
    reconciled
    .filter((F.col("starts_on") > F.to_date(F.lit("2023-07-21"))) & F.col("channel").isNotNull())
    .select(
        "id",
        "channel",
        "promo_code",
        "starts_on",
        "ends_on",
        F.concat_ws("-", "channel", "promo_code").alias("channel_promo_code_key"),
        F.upper("promo_code").alias("promo_code_upper"),
        F.coalesce("channel", F.lit("OTHER")).alias("channel_clean"),
    )
)

valid = (
    base
    .filter((F.col("starts_on") >= F.lit("2023-04-29").cast("date")) & (~F.col("channel").eqNullSafe(F.lit("mobile"))))
    .select(
        "*",
        F.last_day("ends_on").alias("ends_on_month_end"),
        F.coalesce("channel", F.lit("DIRECT")).alias("channel_clean_2"),
    )
)

cleaned = (
    valid
    .filter(F.col("channel").isNotNull())
    .select(
        "id",
        "promo_code",
        "starts_on",
        "ends_on",
        "channel_promo_code_key",
        "promo_code_upper",
        "channel_clean",
        "channel_clean_2",
        F.when(F.col("channel") == "mobile", 1).otherwise(0).alias("is_mobile"),
    )
)

result = (
    cleaned
    .select(
        "promo_code",
        "starts_on",
        "ends_on",
        "channel_promo_code_key",
        "promo_code_upper",
        "channel_clean_2",
        "is_mobile",
    )
)
