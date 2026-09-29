from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel").orderBy(F.col("ends_on").desc(), "id", "promo_id", "promo_id_2")

cleaned = (
    df_promotions
    .filter(F.col("channel").eqNullSafe(F.lit("mobile")))
    .withColumn("discount_pct_whole", F.ceil("discount_pct"))
    .withColumn("promo_code_upper", F.upper("promo_code"))
)

attributed = (
    cleaned
    .alias("a")
    .join(df_promotions.alias("pr"), (F.col("a.channel") == F.col("pr.channel")) & (F.col("a.starts_on") >= F.col("pr.starts_on")) & (F.col("a.starts_on") < F.col("pr.ends_on") + F.expr("INTERVAL 30 DAYS")), "left")
    .select(
        "a.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code").alias("promotion_promo_code"),
        F.col("pr.discount_pct").alias("promotion_discount_pct"),
    )
)

attributed_promotions = (
    attributed
    .alias("s")
    .join(df_promotions.alias("pr"), (F.col("s.channel") == F.col("pr.channel")) & (F.col("s.starts_on") >= F.col("pr.starts_on")) & (F.col("s.starts_on") < F.col("pr.ends_on") + F.expr("INTERVAL 30 DAYS")), "left")
    .select(
        "s.*",
        F.col("pr.id").alias("promo_id_2"),
        F.col("pr.promo_code").alias("promotion_promo_code_2"),
        F.col("pr.discount_pct").alias("promotion_discount_pct_2"),
    )
)

latest = (
    attributed_promotions
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
)

filtered = (
    latest
    .filter(F.col("promotion_promo_code").isNotNull())
    .select("*", F.date_sub("starts_on", 52).alias("starts_on_minus_52d"))
)

result = (
    filtered
    .select(
        "id",
        "channel",
        "promo_code",
        "starts_on",
        "ends_on",
        "discount_pct",
        "promo_id",
        "promotion_promo_code",
        "promotion_discount_pct",
        F.when(F.col("channel").rlike("^(partner|store)$"), "PRIORITY").when(F.col("channel").rlike("^(web|mobile)$"), "EXTENDED").otherwise("GROUP_B").alias("channel_zone"),
    )
    .orderBy("starts_on")
)
