from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel", "promo_code").orderBy("discount_pct_scaled", "id")
w2 = Window.partitionBy("promo_code", "channel").orderBy("ends_on", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("promo_code").orderBy("promotion_discount_pct", "id", "promo_id").rowsBetween(-3, Window.currentRow)
w4 = Window.partitionBy("promotion_promo_code").orderBy(F.col("promotion_discount_pct").desc(), "id", "promo_id")

cleaned = (
    df_promotions
    .filter(F.col("channel") == "partner")
    .select("*", F.round((F.col("discount_pct") * 100), 2).alias("discount_pct_scaled"))
)

with_windows = (
    cleaned
    .select(
        "*",
        F.lag("discount_pct").over(w).alias("prev_discount_pct"),
        F.lead("discount_pct_scaled").over(w).alias("next_discount_pct_scaled"),
        F.lag("discount_pct_scaled").over(w).alias("prev_discount_pct_scaled"),
        F.row_number().over(w).alias("event_seq"),
        F.sum("discount_pct_scaled").over(w2).alias("cumulative_discount_pct_scaled"),
    )
)

matched = (
    with_windows
    .alias("a")
    .join(df_promotions.alias("pr"), (F.coalesce(F.col("a.channel"), F.lit("web")) == F.col("pr.channel")) & F.col("a.ends_on").between(F.col("pr.starts_on"), F.col("pr.ends_on")), "inner")
    .select(
        "a.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code").alias("promotion_promo_code"),
        F.col("pr.discount_pct").alias("promotion_discount_pct"),
    )
)

with_windows_promotions = (
    matched
    .select(
        "id",
        "channel",
        "promo_code",
        "prev_discount_pct_scaled",
        "event_seq",
        "promo_id",
        "promotion_promo_code",
        F.avg("prev_discount_pct").over(w3).alias("prev_discount_pct_avg_3"),
        F.row_number().over(w4).alias("event_seq_2"),
    )
)

result = (
    with_windows_promotions
    .groupBy("promotion_promo_code", "channel")
    .pivot("promo_code", ["BUNDLE", "WELCOME5", "SPRING10"])
    .agg(F.max("prev_discount_pct_scaled"))
)
