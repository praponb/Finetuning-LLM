from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel", "promo_code").orderBy(F.col("ends_on_month").asc_nulls_last())
w2 = Window.partitionBy("promo_code").orderBy("discount_pct", "id").rowsBetween(-6, -1)
w3 = Window.partitionBy("channel", "promo_code").orderBy(F.col("discount_pct").desc())

base = (
    df_promotions
    .filter(~F.col("channel").eqNullSafe(F.lit("mobile")))
    .withColumn("ends_on_month", F.month("ends_on"))
)

result = (
    base
    .select(
        "id",
        "channel",
        "starts_on",
        F.cume_dist().over(w).alias("ends_on_month_cume_dist"),
        F.avg("discount_pct").over(w2).alias("discount_pct_avg_6_prior"),
        F.percent_rank().over(w3).alias("discount_pct_percent_rank"),
    )
)
