from pyspark.sql import functions as F

cleaned = (
    df_promotions
    .filter(F.col("channel").isNotNull())
    .select("*", F.substring("promo_code", 1, 4).alias("promo_code_prefix"))
)

result = (
    cleaned
    .select("id", "channel")
    .subtract(
        df_promotions
        .filter(F.col("promo_code").isin(["BUNDLE", "WELCOME5", "SPRING10"]))
        .select("id", "channel")
    )
)
