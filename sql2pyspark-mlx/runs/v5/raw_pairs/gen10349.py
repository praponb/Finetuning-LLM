from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promotion_promo_code", "channel").orderBy("promotion_discount_pct")

cleaned = (
    df_promotions
    .filter(F.col("channel").isNotNull())
    .select(
        "id",
        "channel",
        "starts_on",
        "ends_on",
        F.last_day("ends_on").alias("ends_on_month_end"),
        F.coalesce("channel", F.lit("OTHER")).alias("channel_clean"),
    )
)

attributed = (
    cleaned
    .alias("e")
    .join(df_promotions.alias("pr"), (F.col("e.channel").eqNullSafe(F.col("pr.channel"))) & F.col("e.ends_on_month_end").between(F.col("pr.starts_on"), F.col("pr.ends_on")), "left")
    .select(
        "e.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code"),
        F.col("pr.discount_pct"),
    )
)

attributed_promotions = (
    attributed
    .alias("s")
    .join(df_promotions.alias("pr"), (F.coalesce(F.col("s.channel"), F.lit("web")) == F.col("pr.channel")) & (F.col("s.ends_on_month_end") >= F.col("pr.starts_on")) & (F.col("s.ends_on_month_end") <= F.date_add(F.col("pr.ends_on"), 10)), "left")
    .select(
        "s.*",
        F.col("pr.id").alias("promo_id_2"),
        F.col("pr.promo_code").alias("promotion_promo_code"),
        F.col("pr.discount_pct").alias("promotion_discount_pct"),
    )
)

ranked = (
    attributed_promotions
    .withColumn("row_num", F.rank().over(w))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

with_window = (
    ranked
    .alias("a")
    .join(df_promotions.alias("pr"), (F.col("a.channel") == F.col("pr.channel")) & (F.col("a.ends_on") >= F.col("pr.starts_on")) & (F.col("a.ends_on") <= F.date_add(F.col("pr.ends_on"), 7)), "left")
    .select(
        "a.*",
        F.col("pr.id").alias("promo_id_3"),
        F.col("pr.promo_code").alias("promotion_promo_code_2"),
        F.col("pr.discount_pct").alias("promotion_discount_pct_2"),
    )
)

result = (
    with_window
    .groupBy(
        F.date_trunc("MONTH", "ends_on_month_end").alias("activity_month"),
        F.when(F.col("promotion_discount_pct_2") >= 0.32, "PREMIUM").otherwise("STANDARD").alias("promotion_discount_pct_2_tier"),
    )
    .agg(
        F.count("*").alias("row_count"),
        F.round(F.sum("promo_id_3") / F.nullif(F.count(F.when(F.col("promotion_promo_code_2") == "WELCOME5", 1)), F.lit(0)), 2).alias("promo_id_3_per_welcome5"),
    )
)
