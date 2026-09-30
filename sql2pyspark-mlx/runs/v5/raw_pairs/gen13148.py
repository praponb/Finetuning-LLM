from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo_code", "channel").orderBy(F.col("starts_on").desc())
w2 = Window.partitionBy("promotion_promo_code").orderBy("discount_pct", "id", "promo_id", "promo_id_2").rowsBetween(-2, Window.currentRow)
w3 = Window.partitionBy("promotion_promo_code", "promo_code").orderBy(F.col("last3_max_discount_pct").desc())

latest = (
    df_promotions
    .withColumn("rn", F.rank().over(w))
    .filter(F.col("rn") == 1)
)

matched = (
    latest
    .alias("s")
    .join(df_promotions.alias("pr"), (F.coalesce(F.col("s.channel"), F.lit("web")) == F.col("pr.channel")) & F.col("s.starts_on").between(F.col("pr.starts_on"), F.col("pr.ends_on")), "left")
    .select(
        "s.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code").alias("promotion_promo_code"),
        F.col("pr.discount_pct").alias("promotion_discount_pct"),
    )
)

matched_promotions = (
    matched
    .alias("s")
    .join(df_promotions.alias("pr"), (F.col("s.channel") == F.col("pr.channel")) & (F.col("s.ends_on") >= F.col("pr.starts_on")) & (F.col("s.ends_on") < F.col("pr.ends_on") + F.expr("INTERVAL 7 DAYS")), "left")
    .select(
        "s.*",
        F.col("pr.id").alias("promo_id_2"),
        F.col("pr.promo_code").alias("promotion_promo_code_2"),
        F.col("pr.discount_pct").alias("promotion_discount_pct_2"),
    )
)

metrics = (
    matched_promotions
    .withColumn("last3_min_discount_pct", F.min("discount_pct").over(w2))
    .withColumn("last3_max_discount_pct", F.max("discount_pct").over(w2))
    .withColumn("last3_sum_discount_pct", F.sum("discount_pct").over(w2))
)

latest_promotions = (
    metrics
    .withColumn("latest_rank", F.rank().over(w3))
    .filter(F.col("latest_rank") == 1)
    .select(
        "id",
        "channel",
        "promo_code",
        "ends_on",
        "discount_pct",
        "promo_id",
        "promotion_promo_code",
        "promotion_discount_pct",
        "promo_id_2",
        "promotion_promo_code_2",
        "promotion_discount_pct_2",
        "last3_max_discount_pct",
        "last3_sum_discount_pct",
    )
)

result = (
    latest_promotions
    .select(
        "id",
        "channel",
        "promo_code",
        "ends_on",
        "discount_pct",
        "promo_id",
        "promotion_promo_code",
        "promotion_discount_pct",
        "promo_id_2",
        F.when(F.col("promo_code").rlike("^(BUNDLE|WELCOME5)$"), "EXTENDED").otherwise("PRIORITY").alias("promo_code_zone"),
    )
    .orderBy(F.col("promotion_promo_code").asc_nulls_last())
)
