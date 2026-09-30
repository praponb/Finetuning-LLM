from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo_code").orderBy("ends_on", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("promo_code")
w3 = Window.partitionBy("promo_code").orderBy("channel_length")
w4 = Window.orderBy("channel_length", "id").rowsBetween(-5, -1)
w5 = Window.orderBy(F.col("ends_on").desc(), "id")

base = (
    df_promotions
    .filter(F.col("channel").isNotNull())
    .select(
        "id",
        "channel",
        "promo_code",
        "starts_on",
        "ends_on",
        F.length("channel").alias("channel_length"),
        F.coalesce("channel", F.lit("DIRECT")).alias("channel_clean"),
    )
)

result = (
    base
    .select(
        "*",
        F.sum("channel_length").over(w).alias("running_channel_length"),
        F.stddev_samp("channel_length").over(w2).alias("promo_code_stddev_samp_channel_length"),
        F.rank().over(w3).alias("channel_length_rank"),
        F.avg("channel_length").over(w4).alias("channel_length_avg_5_prior"),
        F.row_number().over(w5).alias("row_seq"),
    )
)
