from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel").orderBy(F.col("discount_pct").desc())
w2 = Window.orderBy("ends_on_period", "id")
w3 = Window.partitionBy("channel").orderBy(F.col("discount_pct").desc(), "id")

base = (
    df_promotions
    .filter((F.col("starts_on") > F.lit("2023-07-15").cast("date")) & (~F.col("channel").eqNullSafe(F.lit("partner"))))
    .select(
        "id",
        "channel",
        "promo_code",
        "starts_on",
        "discount_pct",
        F.date_trunc("year", "ends_on").alias("ends_on_period"),
        F.concat_ws("-", "promo_code", "channel").alias("promo_code_channel_key"),
        F.coalesce("channel", F.lit("UNKNOWN")).alias("channel_clean"),
    )
)

result = (
    base
    .filter(F.col("starts_on") < "2023-03-01")
    .select(
        "id",
        "promo_code",
        "starts_on",
        "discount_pct",
        "channel_clean",
        F.percent_rank().over(w).alias("discount_pct_percent_rank"),
        F.row_number().over(w2).alias("row_seq"),
        F.ntile(10).over(w3).alias("discount_pct_bucket"),
    )
)
