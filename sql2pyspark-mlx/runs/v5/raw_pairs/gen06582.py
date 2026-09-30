from pyspark.sql import functions as F

filtered = (
    df_promotions
    .filter(F.col("channel").isNotNull())
    .select(
        "*",
        F.date_sub("starts_on", 68).alias("starts_on_minus_68d"),
        F.length("channel").alias("channel_length"),
        F.coalesce("channel", F.lit("UNKNOWN")).alias("resolved_channel"),
    )
)

result = (
    filtered
    .select("id")
    .subtract(
        df_promotions
        .filter(F.col("channel") != "store")
        .select("id")
    )
)
