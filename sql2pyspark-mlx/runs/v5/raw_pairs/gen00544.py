from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("days_since_starts_on").desc_nulls_last())
w2 = Window.partitionBy("channel", "promo_code").orderBy(F.col("days_since_starts_on").desc_nulls_first())

filtered = (
    df_promotions
    .filter(~F.col("promo_code").isin(["VIP", "SPRING10", "BUNDLE"]))
    .select(
        "*",
        F.coalesce("channel", F.lit("Unknown")).alias("channel_filled"),
        F.datediff(F.current_date(), "starts_on").alias("days_since_starts_on"),
        F.coalesce("channel", F.lit("DIRECT")).alias("channel_clean"),
    )
)

result = (
    filtered
    .filter(F.col("promo_code") == "WELCOME5")
    .select(
        "id",
        "channel",
        "promo_code",
        "starts_on",
        "channel_clean",
        F.percent_rank().over(w).alias("days_since_starts_on_percent_rank"),
        F.cume_dist().over(w2).alias("days_since_starts_on_cume_dist"),
    )
)
