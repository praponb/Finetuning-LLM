from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel", "promo_code").orderBy(F.col("starts_on_period").desc_nulls_last(), "id")

base = (
    df_promotions
    .filter((~F.col("channel").eqNullSafe(F.lit("store"))) & F.col("channel").isNotNull())
    .withColumn("starts_on_period", F.date_trunc("year", "starts_on"))
    .withColumn("channel_clean", F.coalesce("channel", F.lit("DIRECT")))
)

result = (
    base
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
    .select("id", "channel", "discount_pct", "starts_on_period", "channel_clean")
)
