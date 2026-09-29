from pyspark.sql import functions as F

filtered = (
    df_promotions
    .filter((F.col("ends_on") > F.lit("2023-09-15").cast("date")) & F.col("channel").isNotNull())
    .select(
        "id",
        "promo_code",
        "starts_on",
        F.floor("discount_pct").alias("discount_pct_whole"),
        F.round("discount_pct", 0).alias("discount_pct_rounded"),
    )
)

result = (
    filtered
    .cube(
        F.when(F.col("discount_pct_whole") >= 61, "LARGE").otherwise("SMALL").alias("discount_pct_whole_tier"),
        F.date_trunc("MONTH", "starts_on").alias("activity_month"),
    )
    .agg(
        F.count("*").alias("event_count"),
        F.countDistinct("promo_code").alias("distinct_promo_codes"),
        F.countDistinct("id").alias("distinct_ids"),
    )
    .filter(F.col("distinct_promo_codes") >= 1)
    .orderBy("activity_month")
)
