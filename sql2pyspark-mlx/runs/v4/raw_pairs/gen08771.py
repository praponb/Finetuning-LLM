from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo_code").orderBy(F.col("starts_on").desc(), "id")
w2 = Window.partitionBy("quarter_start").orderBy(F.col("num_promotion_promo_codes_2").desc())

valid = (
    df_promotions
    .filter(F.col("starts_on") >= F.lit("2023-11-04").cast("date"))
    .withColumn("ends_on_month_end", F.last_day("ends_on"))
    .withColumn("ends_on_dayofweek", F.dayofweek("ends_on"))
    .withColumn("channel_clean", F.coalesce("channel", F.lit("OTHER")))
)

ranked = (
    valid
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
)

with_window = (
    ranked
    .alias("a")
    .join(df_promotions.alias("pr"), (F.coalesce(F.col("a.channel"), F.lit("web")) == F.col("pr.channel")) & F.col("a.ends_on").between(F.col("pr.starts_on"), F.col("pr.ends_on")), "left")
    .select(
        "a.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code").alias("promotion_promo_code"),
        F.col("pr.discount_pct").alias("promotion_discount_pct"),
    )
)

result = (
    with_window
    .groupBy(
        F.date_trunc("quarter", "ends_on").alias("quarter_start"),
        "promotion_promo_code",
        "channel",
    )
    .agg(
        F.sum(F.when(F.col("promo_code") == "WELCOME5", F.col("ends_on_dayofweek")).otherwise(0)).alias("welcome5_ends_on_dayofweek"),
        F.round(F.sum("ends_on_dayofweek") / F.nullif(F.count(F.when(F.col("channel") == "web", 1)), F.lit(0)), 2).alias("ends_on_dayofweek_per_web"),
        F.size(F.collect_set("promotion_promo_code")).alias("num_promotion_promo_codes"),
        F.size(F.collect_set("promotion_promo_code")).alias("num_promotion_promo_codes_2"),
    )
    .withColumn("num_promotion_promo_codes_2_rank", F.rank().over(w2))
    .orderBy(F.col("ends_on_dayofweek_per_web").asc_nulls_last())
)
