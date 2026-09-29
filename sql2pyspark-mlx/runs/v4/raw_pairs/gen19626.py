from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel").orderBy(F.col("discount_pct").desc(), "id")

ranked = (
    df_promotions
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
)

matched = (
    ranked
    .alias("src")
    .join(df_promotions.alias("pr"), (F.coalesce(F.col("src.channel"), F.lit("web")) == F.col("pr.channel")) & F.col("src.starts_on").between(F.col("pr.starts_on"), F.col("pr.ends_on")), "left")
    .select(
        "src.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code").alias("promotion_promo_code"),
        F.col("pr.discount_pct").alias("promotion_discount_pct"),
    )
)

base = (
    matched
    .filter((F.col("starts_on") >= F.to_date(F.lit("2023-11-09"))) & F.col("channel").isNotNull() & (F.col("ends_on") < "2023-10-16"))
    .select(
        "id",
        "channel",
        "promo_code",
        "starts_on",
        "ends_on",
        "promo_id",
        "promotion_discount_pct",
        F.col("promotion_discount_pct").cast("int").alias("promotion_discount_pct_int"),
        F.datediff("starts_on", "ends_on").alias("days_ends_on_to_starts_on"),
        F.coalesce("channel", F.lit("OTHER")).alias("channel_clean"),
    )
)

result = (
    base
    .select(
        "id",
        "channel",
        "promo_code",
        "starts_on",
        "ends_on",
        "promo_id",
        "promotion_discount_pct",
        "days_ends_on_to_starts_on",
        "channel_clean",
        F.when(F.col("promo_code").rlike("^(SPRING10|VIP)$"), "CORE").when(F.col("promo_code").rlike("^(BUNDLE|WELCOME5)$"), "OTHER_ZONE").otherwise("GROUP_B").alias("promo_code_zone"),
    )
    .orderBy("days_ends_on_to_starts_on")
)
