from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel").orderBy(F.col("discount_pct").desc(), "id")
w2 = Window.partitionBy("channel").orderBy(F.col("ends_on").desc(), "id")
w3 = Window.orderBy("latest_rank", "id", "promo_id", "promo_id_2").rowsBetween(-5, Window.currentRow)
w4 = Window.partitionBy("promotion_promo_code").orderBy("promotion_discount_pct", "id", "promo_id", "promo_id_2")

valid = (
    df_promotions
    .filter(F.col("ends_on") >= F.lit("2023-07-25").cast("date"))
    .select(
        "id",
        "channel",
        "starts_on",
        "ends_on",
        "discount_pct",
        F.when(F.col("discount_pct") >= 0.36, "High").when(F.col("discount_pct") >= 0.17, "Medium").otherwise("Low").alias("discount_pct_tier"),
    )
)

ranked = (
    valid
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
)

deduped = (
    ranked
    .withColumn("latest_rank", F.row_number().over(w2))
    .filter(F.col("latest_rank") == 1)
)

attributed = (
    deduped
    .alias("e")
    .join(df_promotions.alias("pr"), (F.col("e.channel") == F.col("pr.channel")) & F.col("e.ends_on").between(F.col("pr.starts_on"), F.col("pr.ends_on")), "left")
    .select(
        "e.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code"),
        F.col("pr.discount_pct").alias("promotion_discount_pct"),
    )
)

with_window = (
    attributed
    .alias("e")
    .join(df_promotions.alias("pr"), (F.col("e.channel") == F.col("pr.channel")) & (F.col("e.starts_on") >= F.col("pr.starts_on")) & (F.col("e.starts_on") < F.col("pr.ends_on") + F.expr("INTERVAL 30 DAYS")), "inner")
    .select(
        "e.*",
        F.col("pr.id").alias("promo_id_2"),
        F.col("pr.promo_code").alias("promotion_promo_code"),
        F.col("pr.discount_pct").alias("promotion_discount_pct_2"),
    )
)

result = (
    with_window
    .select(
        "id",
        "channel",
        "discount_pct",
        "rn",
        "latest_rank",
        "promo_id",
        "promotion_discount_pct",
        "promo_id_2",
        "promotion_promo_code",
        F.avg("promo_id_2").over(w3).alias("promo_id_2_avg_5"),
        F.ntile(3).over(w4).alias("promotion_discount_pct_bucket"),
    )
)
