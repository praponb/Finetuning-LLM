from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo_code").orderBy(F.col("discount_pct").desc(), "id")
w2 = Window.partitionBy("channel", "promo_code").orderBy(F.col("ends_on").desc(), "id")

ranked = (
    df_promotions
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
)

latest = (
    ranked
    .withColumn("latest_rank", F.row_number().over(w2))
    .filter(F.col("latest_rank") == 1)
)

cleaned = (
    latest
    .filter(F.col("channel").isNotNull())
    .select(
        "*",
        F.when(F.col("promo_code").isin(["SPRING10", "VIP"]), "Core").otherwise("Other").alias("promo_code_group"),
        F.coalesce("channel", F.lit("OTHER")).alias("resolved_channel"),
    )
)

with_window = (
    cleaned
    .alias("a")
    .join(df_promotions.alias("pr"), (F.coalesce(F.col("a.channel"), F.lit("store")) == F.col("pr.channel")) & F.col("a.starts_on").between(F.col("pr.starts_on"), F.col("pr.ends_on")), "left")
    .select(
        "a.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code").alias("promotion_promo_code"),
        F.col("pr.discount_pct").alias("promotion_discount_pct"),
    )
)

with_window_promotions = (
    with_window
    .alias("a")
    .join(df_promotions.alias("pr"), (F.coalesce(F.col("a.channel"), F.lit("web")) == F.col("pr.channel")) & F.col("a.starts_on").between(F.col("pr.starts_on"), F.col("pr.ends_on")), "left")
    .select(
        "a.*",
        F.col("pr.id").alias("promo_id_2"),
        F.col("pr.promo_code").alias("promotion_promo_code_2"),
        F.col("pr.discount_pct").alias("promotion_discount_pct_2"),
    )
)

result = (
    with_window_promotions
    .groupBy("promotion_promo_code", "channel")
    .pivot("promo_code", ["BUNDLE", "SPRING10", "WELCOME5"])
    .agg(F.avg("promo_id_2").alias("total"), F.count(F.lit(1)).alias("n"))
)
