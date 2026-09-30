from pyspark.sql import functions as F

cleaned = (
    df_promotions
    .filter(F.col("channel").isNull() & F.col("channel").isNotNull())
    .select(
        "id",
        "channel",
        "promo_code",
        "starts_on",
        "ends_on",
        F.lower("promo_code").alias("promo_code_lower"),
        F.coalesce("channel", F.lit("NONE")).alias("channel_filled"),
    )
)

result = (
    cleaned
    .withColumn("year_start", F.date_trunc("YEAR", "ends_on"))
    .groupingSets(
        [["promo_code", "channel", "year_start"], ["promo_code", "channel"], ["promo_code"], []],
        "promo_code",
        "channel",
        "year_start",
    )
    .agg(
        F.countDistinct("promo_code").alias("distinct_promo_codes"),
        F.count("*").alias("row_count"),
        F.grouping_id().alias("grouping_level"),
    )
    .orderBy(F.col("distinct_promo_codes").asc_nulls_first())
)
