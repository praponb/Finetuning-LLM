from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel", "promotion_promo_code")
w2 = Window.partitionBy("promo_code").orderBy(F.col("discount_pct_scaled").desc(), "id", "promo_id")
w3 = Window.partitionBy("promotion_promo_code")
w4 = Window.partitionBy("channel").orderBy(F.col("event_seq").asc_nulls_last(), "id", "promo_id")

base = (
    df_promotions
    .filter((~F.col("channel").eqNullSafe(F.lit("web"))) & F.col("channel").isNotNull())
    .select(
        "*",
        F.upper("channel").alias("channel_upper"),
        F.round((F.col("discount_pct") * 0.9), 2).alias("discount_pct_scaled"),
    )
)

matched = (
    base
    .alias("src")
    .join(df_promotions.alias("pr"), (F.coalesce(F.col("src.channel"), F.lit("web")) == F.col("pr.channel")) & F.col("src.starts_on").between(F.col("pr.starts_on"), F.col("pr.ends_on")), "left")
    .select(
        "src.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code").alias("promotion_promo_code"),
        F.col("pr.discount_pct").alias("promotion_discount_pct"),
    )
)

scored = (
    matched
    .withColumn("channel_avg_discount_pct_scaled", F.avg("discount_pct_scaled").over(w))
    .withColumn("event_seq", F.row_number().over(w2))
    .withColumn("promotion_promo_code_min_promo_id", F.min("promo_id").over(w3))
)

latest = (
    scored
    .withColumn("latest_rank", F.row_number().over(w4))
    .filter(F.col("latest_rank") == 1)
    .drop("latest_rank")
)

result = (
    latest
    .groupBy("promotion_promo_code")
    .pivot("promo_code", ["VIP", "SPRING10", "WELCOME5"])
    .agg(F.avg("discount_pct"))
    .select(
        "promotion_promo_code",
        F.col("VIP").alias("vip"),
        F.col("SPRING10").alias("spring10"),
        F.col("WELCOME5").alias("welcome5"),
    )
)
