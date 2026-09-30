from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo_code")
w2 = Window.orderBy("discount_pct", "id").rowsBetween(-2, -1)

filtered = (
    df_promotions
    .filter(F.col("channel").isNotNull())
    .select(
        "id",
        "channel",
        "promo_code",
        "starts_on",
        "ends_on",
        "discount_pct",
        F.length("channel").alias("channel_length"),
        F.concat_ws(",", "promo_code", "channel").alias("promo_code_channel_key"),
        F.coalesce("channel", F.lit("N/A")).alias("channel_filled"),
    )
)

result = (
    filtered
    .withColumn("discount_pct_share", F.round((F.col("discount_pct") / F.sum("discount_pct").over(w)), 4))
    .withColumn("channel_length_avg_2_prior", F.avg("channel_length").over(w2))
)
