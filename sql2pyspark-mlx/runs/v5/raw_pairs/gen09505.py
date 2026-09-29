from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel").orderBy("discount_pct", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("channel").orderBy("discount_pct")
w3 = Window.partitionBy("channel")
w4 = Window.partitionBy("promo_code", "channel").orderBy(F.col("discount_pct").desc(), "id")
w5 = Window.partitionBy("promotion_promo_code").orderBy("discount_pct_bucket", "id", "promo_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("promo_code", "channel")
w7 = Window.partitionBy("channel").orderBy(F.col("discount_pct_rank").desc(), "id", "promo_id")

enriched = (
    df_promotions
    .filter(F.col("starts_on").between("2025-09-01", "2025-10-01"))
    .withColumn("first_known_channel", F.first("channel", ignorenulls=True).over(w))
    .withColumn("discount_pct_rank", F.rank().over(w2))
    .withColumn("discount_pct_share", F.round((F.col("discount_pct") / F.sum("discount_pct").over(w3)), 4))
    .withColumn("discount_pct_bucket", F.ntile(10).over(w4))
)

attributed = (
    enriched
    .alias("a")
    .join(df_promotions.alias("pr"), (F.col("a.channel") == F.col("pr.channel")) & F.col("a.starts_on").between(F.col("pr.starts_on"), F.col("pr.ends_on")), "inner")
    .select(
        "a.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code").alias("promotion_promo_code"),
        F.col("pr.discount_pct").alias("promotion_discount_pct"),
    )
)

cleaned = (
    attributed
    .filter(F.col("promotion_promo_code").isNotNull())
    .withColumn("discount_pct_rounded", F.round("discount_pct", 1))
    .withColumn("channel_clean", F.coalesce("channel", F.lit("UNKNOWN")))
)

filtered = (
    cleaned
    .filter((F.col("ends_on") >= F.lit("2023-07-21").cast("date")) & F.col("promotion_promo_code").isNotNull())
    .withColumn("promo_code_lower", F.lower("promo_code"))
    .withColumn("channel_clean_2", F.coalesce("channel", F.lit("NONE")))
)

result = (
    filtered
    .select(
        "id",
        "promo_code",
        "ends_on",
        "discount_pct",
        "discount_pct_rank",
        "discount_pct_bucket",
        "promo_id",
        "promotion_discount_pct",
        "promo_code_lower",
        "channel_clean_2",
        F.sum("discount_pct_rank").over(w5).alias("cumulative_discount_pct_rank"),
        F.sum("discount_pct_rounded").over(w6).alias("promo_code_sum_discount_pct_rounded"),
        F.row_number().over(w7).alias("row_seq"),
    )
)
