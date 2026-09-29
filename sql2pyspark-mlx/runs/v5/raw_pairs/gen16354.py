from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.partitionBy("promo_code").orderBy(F.col("discount_pct").desc(), "id")
w2 = Window.partitionBy("channel").orderBy("starts_on", "id").rowsBetween(-5, Window.currentRow)
w3 = Window.partitionBy("channel").orderBy("starts_on", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_grp = Window.orderBy(F.col("discount_pct").desc())

with_windows = (
    df_promotions
    .withColumn("discount_pct_bucket", F.ntile(5).over(w_part))
    .withColumn("discount_pct_avg_5", F.avg("discount_pct").over(w2))
    .withColumn("last_known_channel", F.last("channel", ignorenulls=True).over(w3))
    .withColumn("discount_pct_bucket_2", F.ntile(3).over(w_part))
    .withColumn("discount_pct_cume_dist", F.cume_dist().over(w_grp))
)

valid = (
    with_windows
    .filter(F.col("promo_code").isin(["WELCOME5", "SPRING10", "VIP"]))
    .select(
        "*",
        F.round("discount_pct_bucket", 0).alias("discount_pct_bucket_rounded"),
        F.add_months("starts_on", 3).alias("starts_on_plus_months"),
        F.coalesce("channel", F.lit("DIRECT")).alias("channel_filled"),
    )
)

base = (
    valid
    .filter((F.col("ends_on") > F.to_date(F.lit("2023-08-28"))) & F.col("channel").isNotNull())
    .select(
        "*",
        F.ceil("discount_pct").alias("discount_pct_whole"),
        F.coalesce("channel", F.lit("DIRECT")).alias("channel_filled_2"),
    )
)

matched = (
    base
    .alias("src")
    .join(df_promotions.alias("pr"), (F.coalesce(F.col("src.channel"), F.lit("store")) == F.col("pr.channel")) & (F.col("src.ends_on") >= F.col("pr.starts_on")) & (F.col("src.ends_on") < F.col("pr.ends_on") + F.expr("INTERVAL 14 DAYS")), "left")
    .select(
        "src.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code").alias("promotion_promo_code"),
        F.col("pr.discount_pct").alias("promotion_discount_pct"),
    )
)

result = (
    matched
    .select(
        "id",
        "promo_id",
        "channel",
        "discount_pct",
        F.col("discount_pct_bucket_rounded").alias("DISCOUNT_PCT_BUCKET_ROUNDED"),
        F.col("promotion_discount_pct").alias("PROMOTION_DISCOUNT_PCT_VAL"),
    )
    .unpivot(
        ids=["id", "promo_id", "channel"],
        values=["discount_pct", "DISCOUNT_PCT_BUCKET_ROUNDED", "PROMOTION_DISCOUNT_PCT_VAL"],
        variableColumnName="kpi",
        valueColumnName="val",
    )
    .filter(F.col("val").isNotNull())
)
