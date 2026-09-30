from pyspark.sql import functions as F

valid = (
    df_promotions
    .filter((F.col("starts_on") > F.lit("2023-04-17").cast("date")) & F.col("channel").isNotNull())
    .select(
        "id",
        "channel",
        "promo_code",
        "starts_on",
        "ends_on",
        "discount_pct",
        F.date_format("ends_on", "yyyy-MM").alias("ends_on_label"),
        F.datediff(F.current_date(), "starts_on").alias("days_since_starts_on"),
    )
)

kept_keys = (
    valid
    .select("channel")
    .subtract(
        df_promotions
        .filter(F.col("starts_on").between("2023-03-01", "2024-08-01"))
        .select("channel")
    )
)

verified = (
    valid
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.channel") == F.col("k.channel")), "inner")
    .select("s.*")
)

cleaned = (
    verified
    .filter((F.col("ends_on") > F.to_date(F.lit("2023-08-10"))) & F.col("channel").isNotNull())
    .withColumn("promo_code_upper", F.upper("promo_code"))
    .withColumn("resolved_channel", F.coalesce("channel", F.lit("N/A")))
)

result = (
    cleaned
    .select(
        "channel",
        "promo_code",
        "starts_on",
        "ends_on",
        "ends_on_label",
        "days_since_starts_on",
        F.when(F.col("promo_code").rlike("^(BUNDLE|WELCOME5)$"), "GROUP_A").otherwise("PRIORITY").alias("promo_code_zone"),
    )
    .orderBy(F.col("starts_on").desc_nulls_last())
)
