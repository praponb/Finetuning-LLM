from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo_code", "channel").orderBy(F.col("starts_on").desc(), "id")

ranked = (
    df_promotions
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
)

result = (
    ranked
    .groupBy("promo_code")
    .agg(
        F.size(F.collect_set("promo_code")).alias("num_promo_codes"),
        F.percentile_approx("discount_pct", [0.25, 0.5, 0.75]).alias("discount_pct_quartiles"),
        F.percentile_approx("discount_pct", 0.95).alias("discount_pct_p95"),
        F.count_if(F.col("promo_code") == "VIP").alias("vip_events"),
    )
)
