from pyspark.sql import functions as F

base = (
    df_promotions
    .filter(F.col("ends_on") >= F.lit("2023-07-30").cast("date"))
    .select(
        "*",
        F.datediff("ends_on", "starts_on").alias("days_starts_on_to_ends_on"),
        F.concat("channel", F.lit(" - "), "promo_code").alias("channel_promo_code"),
    )
)

result = (
    base
    .groupBy(F.date_trunc("year", "ends_on").alias("year_start"))
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.countDistinct("promo_code").alias("distinct_promo_codes"),
    )
    .orderBy(F.col("distinct_promo_codes").asc_nulls_last())
)
