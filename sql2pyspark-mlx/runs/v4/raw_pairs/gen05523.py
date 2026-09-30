from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel", "promotion_promo_code").orderBy(F.col("discount_pct").desc(), "id", "promo_id")
w2 = Window.partitionBy("promo_code").orderBy(F.col("promotion_discount_pct_2").asc_nulls_last(), "id", "promo_id", "promo_id_2")

filtered = (
    df_promotions
    .filter(F.col("starts_on") > "2023-03-01")
    .select(
        "*",
        F.date_trunc("month", "ends_on").alias("ends_on_period"),
        F.trim("channel").alias("clean_channel"),
    )
)

matched = (
    filtered
    .alias("e")
    .join(df_promotions.alias("pr"), (F.col("e.channel") == F.col("pr.channel")) & (F.col("e.starts_on") >= F.col("pr.starts_on")) & (F.col("e.starts_on") <= F.date_add(F.col("pr.ends_on"), 3)), "inner")
    .select(
        "e.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code").alias("promotion_promo_code"),
        F.col("pr.discount_pct").alias("promotion_discount_pct"),
    )
)

deduped = (
    matched
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
)

with_window = (
    deduped
    .alias("src")
    .join(df_promotions.alias("pr"), (F.col("src.channel").eqNullSafe(F.col("pr.channel"))) & (F.col("src.starts_on") >= F.col("pr.starts_on")) & (F.col("src.starts_on") <= F.date_add(F.col("pr.ends_on"), 10)), "left")
    .select(
        "src.*",
        F.col("pr.id").alias("promo_id_2"),
        F.col("pr.promo_code").alias("promotion_promo_code_2"),
        F.col("pr.discount_pct").alias("promotion_discount_pct_2"),
    )
)

ranked = (
    with_window
    .withColumn("row_num_2", F.row_number().over(w2))
    .filter(F.col("row_num_2") == 1)
)

base = (
    ranked
    .filter(F.col("ends_on") > F.lit("2023-06-29").cast("date"))
    .select(
        "id",
        "channel",
        "starts_on",
        "ends_on",
        "discount_pct",
        "clean_channel",
        "promo_id",
        "promotion_promo_code",
        "promotion_discount_pct",
        "row_num",
        "promo_id_2",
        "promotion_promo_code_2",
        "promotion_discount_pct_2",
        "row_num_2",
        F.coalesce("promo_id_2", F.lit(0)).alias("promo_id_2_filled"),
        F.coalesce("channel", F.lit("OTHER")).alias("channel_filled"),
    )
)

matched_promotions = (
    base
    .alias("s")
    .join(df_promotions.alias("pr"), (F.col("s.channel") == F.col("pr.channel")) & (F.col("s.ends_on") >= F.col("pr.starts_on")) & (F.col("s.ends_on") <= F.date_add(F.col("pr.ends_on"), 3)), "inner")
    .select(
        "s.*",
        F.col("pr.id").alias("promo_id_3"),
        F.col("pr.promo_code"),
        F.col("pr.discount_pct").alias("promotion_discount_pct_3"),
    )
)

grouped = (
    matched_promotions
    .groupBy(F.date_trunc("quarter", "ends_on").alias("quarter_start"))
    .agg(
        F.count("*").alias("cnt"),
        F.count(F.when(F.col("promotion_promo_code") == "WELCOME5", 1)).alias("welcome5_count"),
    )
    .filter(F.col("welcome5_count") >= 1)
)

result = (
    grouped
    .select("quarter_start", "welcome5_count")
    .orderBy(F.col("welcome5_count").desc_nulls_last())
)
