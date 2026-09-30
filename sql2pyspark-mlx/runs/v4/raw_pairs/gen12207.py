from pyspark.sql import functions as F

base = (
    df_promotions
    .filter((F.col("ends_on") > F.lit("2023-10-04").cast("date")) & F.col("channel").isNotNull())
    .select(
        "*",
        F.ceil("discount_pct").alias("discount_pct_whole"),
        F.upper("promo_code").alias("promo_code_upper"),
    )
)

result = (
    base
    .select("channel", "promo_code")
    .exceptAll(
        df_promotions
        .filter(F.col("discount_pct") < 0.17)
        .select("channel", "promo_code")
    )
)
