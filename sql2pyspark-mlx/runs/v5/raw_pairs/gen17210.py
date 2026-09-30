from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel", "promo_code").orderBy("ends_on")

ranked = (
    df_promotions
    .withColumn("latest_rank", F.rank().over(w))
    .filter(F.col("latest_rank") == 1)
)

result = (
    ranked
    .withColumn("year_start", F.date_trunc("YEAR", "ends_on"))
    .withColumn("week_start", F.date_trunc("WEEK", "starts_on"))
    .withColumn("discount_pct_tier", F.when(F.col("discount_pct") >= 0.35, "HIGH").otherwise("LOW"))
    .groupingSets(
        [["year_start", "week_start", "discount_pct_tier"], ["year_start", "week_start"], ["year_start"]],
        "year_start",
        "week_start",
        "discount_pct_tier",
    )
    .agg(
        F.stddev_samp("discount_pct").alias("discount_pct_stddev"),
        F.count("*").alias("event_count"),
        F.countDistinct("channel").alias("distinct_channels"),
        F.grouping_id().alias("grouping_id"),
    )
    .orderBy(F.col("discount_pct_stddev").desc())
)
