from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo_code", "channel").orderBy("ends_on")
w2 = Window.partitionBy("promotion_promo_code_3").orderBy(F.col("promo_id_2").desc())
w3 = Window.orderBy("promo_id", "id", "promo_id", "promo_id_2", "promo_id_3")
w4 = Window.orderBy(F.col("promo_id").desc(), "id", "promo_id", "promo_id_2", "promo_id_3")
w5 = Window.partitionBy("promo_code")

cleaned = (
    df_promotions
    .filter((F.col("ends_on") >= F.to_date(F.lit("2023-10-02"))) & F.col("channel").isNotNull())
    .select(
        "id",
        "channel",
        "promo_code",
        "ends_on",
        "discount_pct",
        F.trim("promo_code").alias("clean_promo_code"),
        F.coalesce("channel", F.lit("N/A")).alias("channel_filled"),
    )
)

ranked = (
    cleaned
    .withColumn("latest_rank", F.rank().over(w))
    .filter(F.col("latest_rank") == 1)
    .drop("latest_rank")
)

attributed = (
    ranked
    .alias("a")
    .join(df_promotions.alias("pr"), (F.coalesce(F.col("a.channel"), F.lit("web")) == F.col("pr.channel")) & (F.col("a.ends_on") >= F.col("pr.starts_on")) & (F.col("a.ends_on") < F.col("pr.ends_on") + F.expr("INTERVAL 14 DAYS")), "left")
    .select(
        "a.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code").alias("promotion_promo_code"),
        F.col("pr.discount_pct").alias("promotion_discount_pct"),
    )
)

matched = (
    attributed
    .alias("src")
    .join(df_promotions.alias("pr"), (F.coalesce(F.col("src.channel"), F.lit("web")) == F.col("pr.channel")) & (F.col("src.ends_on") >= F.col("pr.starts_on")) & (F.col("src.ends_on") <= F.date_add(F.col("pr.ends_on"), 10)), "left")
    .select(
        "src.*",
        F.col("pr.id").alias("promo_id_2"),
        F.col("pr.promo_code").alias("promotion_promo_code_2"),
        F.col("pr.discount_pct").alias("promotion_discount_pct_2"),
    )
)

matched_promotions = (
    matched
    .alias("e")
    .join(df_promotions.alias("pr"), (F.col("e.channel") == F.col("pr.channel")) & F.col("e.ends_on").between(F.col("pr.starts_on"), F.col("pr.ends_on")), "left")
    .select(
        "e.*",
        F.col("pr.id").alias("promo_id_3"),
        F.col("pr.promo_code").alias("promotion_promo_code_3"),
        F.col("pr.discount_pct").alias("promotion_discount_pct_3"),
    )
)

result = (
    matched_promotions
    .select(
        "*",
        F.percent_rank().over(w2).alias("promo_id_2_percent_rank"),
        F.lag("promotion_discount_pct_3", 1, 0).over(w3).alias("prev_promotion_discount_pct_3"),
        F.row_number().over(w4).alias("seq_no"),
        F.sum("promotion_discount_pct_2").over(w5).alias("promo_code_sum_promotion_discount_pct_2"),
    )
)
