from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.partitionBy("channel").orderBy("promotion_discount_pct", "id", "promo_id").rowsBetween(-6, Window.currentRow)

filtered = (
    df_promotions
    .filter((F.col("starts_on") >= F.to_date(F.lit("2023-09-12"))) & F.col("channel").isNotNull())
    .select(
        "id",
        "channel",
        "promo_code",
        "starts_on",
        "ends_on",
        "discount_pct",
        F.round((F.col("discount_pct") * 0.9), 2).alias("adjusted_discount_pct"),
    )
)

filtered_promotions = (
    filtered
    .filter((F.col("starts_on") >= F.lit("2023-09-11").cast("date")) & (F.col("ends_on") <= "2025-07-08"))
    .withColumn("channel_group", F.when(F.col("channel").isin(["web", "mobile"]), "Group A").otherwise("Other"))
)

with_window = (
    filtered_promotions
    .alias("s")
    .join(df_promotions.alias("pr"), (F.coalesce(F.col("s.channel"), F.lit("store")) == F.col("pr.channel")) & (F.col("s.ends_on") >= F.col("pr.starts_on")) & (F.col("s.ends_on") <= F.date_add(F.col("pr.ends_on"), 7)), "left")
    .select(
        "s.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code").alias("promotion_promo_code"),
        F.col("pr.discount_pct").alias("promotion_discount_pct"),
    )
)

metrics = (
    with_window
    .select(
        "id",
        "channel",
        "promo_code",
        "starts_on",
        "adjusted_discount_pct",
        "promo_id",
        "promotion_discount_pct",
        F.min("promo_id").over(w_grp).alias("last7_min_promo_id"),
        F.avg("promo_id").over(w_grp).alias("last7_avg_promo_id"),
        F.max("promo_id").over(w_grp).alias("last7_max_promo_id"),
        F.sum("promo_id").over(w_grp).alias("last7_sum_promo_id"),
    )
)

matched = (
    metrics
    .alias("s")
    .join(df_promotions.alias("pr"), (F.coalesce(F.col("s.channel"), F.lit("store")) == F.col("pr.channel")) & (F.col("s.starts_on") >= F.col("pr.starts_on")) & (F.col("s.starts_on") <= F.date_add(F.col("pr.ends_on"), 3)), "left")
    .select(
        "s.*",
        F.col("pr.id").alias("promo_id_2"),
        F.col("pr.promo_code").alias("promotion_promo_code"),
        F.col("pr.discount_pct"),
    )
)

result = (
    matched
    .select(
        "id",
        "channel",
        "promo_code",
        "promo_id",
        "last7_min_promo_id",
        "last7_avg_promo_id",
        "last7_max_promo_id",
        "last7_sum_promo_id",
        "promo_id_2",
        F.when(F.col("channel").rlike("^(partner|web)$"), "CORE").when(F.col("channel").rlike("^(mobile|store)$"), "GROUP_B").otherwise("PRIORITY").alias("channel_zone"),
    )
    .orderBy(F.col("last7_max_promo_id").desc_nulls_first())
)
