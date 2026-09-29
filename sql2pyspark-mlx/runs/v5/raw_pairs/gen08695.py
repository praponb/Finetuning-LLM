from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo_code").orderBy("starts_on", "id")
w2 = Window.partitionBy("promotion_promo_code").orderBy("discount_pct", "id", "promo_id")
w3 = Window.partitionBy("channel", "promo_code").orderBy("starts_on", "id", "promo_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_promotions
    .filter(F.col("channel").isNotNull())
    .withColumn("channel_promo_code", F.concat("channel", F.lit("/"), "promo_code"))
    .withColumn("channel_filled", F.coalesce("channel", F.lit("N/A")))
)

latest = (
    base
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

matched = (
    latest
    .alias("s")
    .join(df_promotions.alias("pr"), (F.coalesce(F.col("s.channel"), F.lit("store")) == F.col("pr.channel")) & (F.col("s.ends_on") >= F.col("pr.starts_on")) & (F.col("s.ends_on") < F.col("pr.ends_on") + F.expr("INTERVAL 14 DAYS")), "left")
    .select(
        "s.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code").alias("promotion_promo_code"),
        F.col("pr.discount_pct").alias("promotion_discount_pct"),
    )
)

enriched = (
    matched
    .filter(F.col("promo_id").between(6, 8))
    .select(
        "id",
        "channel",
        "ends_on",
        "channel_promo_code",
        "channel_filled",
        "promo_id",
        "promotion_promo_code",
        "promotion_discount_pct",
        F.ntile(10).over(w2).alias("discount_pct_bucket"),
        F.sum("discount_pct").over(w3).alias("running_discount_pct"),
    )
)

matched_promotions = (
    enriched
    .alias("s")
    .join(df_promotions.alias("pr"), (F.coalesce(F.col("s.channel"), F.lit("store")) == F.col("pr.channel")) & (F.col("s.ends_on") >= F.col("pr.starts_on")) & (F.col("s.ends_on") <= F.date_add(F.col("pr.ends_on"), 7)), "left")
    .select(
        "s.*",
        F.col("pr.id").alias("promo_id_2"),
        F.col("pr.promo_code"),
        F.col("pr.discount_pct"),
    )
)

result = (
    matched_promotions
    .select(
        "id",
        "channel",
        "ends_on",
        "channel_filled",
        "promo_id",
        "promotion_discount_pct",
        "running_discount_pct",
        "promo_id_2",
        "promo_code",
    )
    .orderBy(F.col("running_discount_pct").desc())
)
