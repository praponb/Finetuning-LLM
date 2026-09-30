from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo_code", "channel").orderBy(F.col("ends_on").desc(), "id")

latest = (
    df_promotions
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
)

result = (
    latest
    .groupBy(F.date_trunc("MONTH", "ends_on").alias("activity_month"))
    .agg(
        F.size(F.collect_set("promo_code")).alias("num_promo_codes"),
        F.round(F.avg("discount_pct"), 2).alias("avg_discount_pct"),
        F.round(F.sum("discount_pct") / F.nullif(F.count(F.when(F.col("channel") == "mobile", 1)), F.lit(0)), 2).alias("discount_pct_per_mobile"),
        F.stddev_samp("discount_pct").alias("discount_pct_stddev"),
    )
)
