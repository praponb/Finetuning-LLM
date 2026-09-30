from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo_code").orderBy(F.col("discount_pct_rounded").desc_nulls_last(), "id")

base = (
    df_promotions
    .filter(F.col("channel") != "web")
    .select(
        "*",
        F.round("discount_pct", 2).alias("discount_pct_rounded"),
        F.coalesce("channel", F.lit("UNKNOWN")).alias("channel_clean"),
    )
)

result = (
    base
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
)
