from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel").orderBy("ends_on", "id").rowsBetween(-6, -1)
w2 = Window.partitionBy("promo_code", "channel").orderBy("discount_pct", "id")
w_recent = Window.orderBy("promo_id", "id", "promo_id")
w_recent_2 = Window.partitionBy("promotion_promo_code", "promo_code").orderBy(F.col("discount_pct_rounded_avg_6_prior").desc(), "id", "promo_id")
w_hist = Window.partitionBy("promo_code").orderBy("promo_id", "id", "promo_id")
w_ordered = Window.partitionBy("promotion_promo_code").orderBy("promo_id", "id", "promo_id")

valid = (
    df_promotions
    .filter(F.col("discount_pct") <= 0.18)
    .select("*", F.round("discount_pct", 1).alias("discount_pct_rounded"))
)

scored = (
    valid
    .withColumn("discount_pct_rounded_avg_6_prior", F.avg("discount_pct_rounded").over(w))
    .withColumn("discount_pct_bucket", F.ntile(10).over(w2))
)

matched = (
    scored
    .alias("s")
    .join(df_promotions.alias("pr"), (F.col("s.channel") == F.col("pr.channel")) & (F.col("s.ends_on") >= F.col("pr.starts_on")) & (F.col("s.ends_on") < F.col("pr.ends_on") + F.expr("INTERVAL 14 DAYS")), "inner")
    .select(
        "s.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code").alias("promotion_promo_code"),
        F.col("pr.discount_pct").alias("promotion_discount_pct"),
    )
)

with_windows = (
    matched
    .withColumn("prev_discount_pct_bucket", F.lag("discount_pct_bucket", 1).over(w_recent))
    .withColumn("discount_pct_rounded_avg_6_prior_bucket", F.ntile(5).over(w_recent_2))
    .withColumn("prev_discount_pct_rounded", F.lag("discount_pct_rounded", 1, 0).over(w_hist))
    .withColumn("next_discount_pct_rounded", F.lead("discount_pct_rounded", 1).over(w_ordered))
)

summary = (
    with_windows
    .groupBy(
        F.when(F.col("discount_pct") >= 0.3, "LARGE").otherwise("SMALL").alias("discount_pct_tier"),
    )
    .agg(
        F.size(F.collect_set("channel")).alias("num_channels"),
        F.avg(F.when(F.col("promo_code") == "WELCOME5", F.col("prev_discount_pct_rounded"))).alias("welcome5_avg_prev_discount_pct_rounded"),
        F.count("*").alias("cnt"),
    )
)

result = (
    summary
    .select("discount_pct_tier", "num_channels")
    .orderBy("discount_pct_tier", "num_channels")
)
