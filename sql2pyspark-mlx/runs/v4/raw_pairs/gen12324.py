from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo_code").orderBy(F.col("discount_pct_whole").asc_nulls_first())
w_recent = Window.partitionBy("channel").orderBy("discount_pct", "id")

valid = (
    df_promotions
    .filter(F.col("channel").isNotNull())
    .withColumn("discount_pct_whole", F.ceil("discount_pct"))
    .withColumn("channel_title", F.initcap(F.trim("channel")))
    .withColumn("channel_filled", F.coalesce("channel", F.lit("UNKNOWN")))
)

metrics = (
    valid
    .select(
        "*",
        F.dense_rank().over(w).alias("discount_pct_whole_dense_rank"),
        F.lead("discount_pct_whole", 1, 0).over(w_recent).alias("next_discount_pct_whole"),
    )
)

with_window = (
    metrics
    .alias("s")
    .join(df_promotions.alias("pr"), (F.col("s.channel") == F.col("pr.channel")) & (F.col("s.ends_on") >= F.col("pr.starts_on")) & (F.col("s.ends_on") < F.col("pr.ends_on") + F.expr("INTERVAL 7 DAYS")), "left")
    .select(
        "s.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code").alias("promotion_promo_code"),
        F.col("pr.discount_pct").alias("promotion_discount_pct"),
    )
)

with_window_promotions = (
    with_window
    .alias("e")
    .join(df_promotions.alias("pr"), (F.coalesce(F.col("e.channel"), F.lit("web")) == F.col("pr.channel")) & (F.col("e.ends_on") >= F.col("pr.starts_on")) & (F.col("e.ends_on") < F.col("pr.ends_on") + F.expr("INTERVAL 7 DAYS")), "inner")
    .select(
        "e.*",
        F.col("pr.id").alias("promo_id_2"),
        F.col("pr.promo_code").alias("promotion_promo_code_2"),
        F.col("pr.discount_pct").alias("promotion_discount_pct_2"),
    )
)

result = (
    with_window_promotions
    .select(
        "id",
        "channel",
        "promo_code",
        "starts_on",
        "discount_pct",
        "discount_pct_whole",
        "channel_title",
        "channel_filled",
        "discount_pct_whole_dense_rank",
        F.when(F.col("channel").rlike("^(web|partner)$"), "GROUP_A").when(F.col("channel").rlike("^(mobile|store)$"), "EXTENDED").otherwise("PRIORITY").alias("channel_zone"),
    )
    .orderBy(F.col("discount_pct_whole_dense_rank").desc_nulls_first())
)
