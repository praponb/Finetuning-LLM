from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo_code").orderBy("starts_on", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("promo_code").orderBy("ends_on", "id").rowsBetween(-2, -1)
w3 = Window.orderBy("discount_pct", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_promotions
    .filter((F.col("ends_on") >= F.lit("2023-06-29").cast("date")) & (F.col("channel") == "web"))
    .withColumn("discount_pct_tier", F.when(F.col("discount_pct") >= 0.34, "High").when(F.col("discount_pct") >= 0.12, "Medium").otherwise("Low"))
    .withColumn("starts_on_label", F.date_format("starts_on", "yyyy-MM-dd"))
)

filtered = (
    cleaned
    .filter((F.col("starts_on") >= F.lit("2023-06-22").cast("date")) & F.col("channel").isNotNull())
    .select(
        "id",
        "channel",
        "promo_code",
        "starts_on",
        "ends_on",
        "discount_pct",
        "discount_pct_tier",
        "starts_on_label",
        F.date_format("ends_on", "yyyy-MM-dd").alias("ends_on_label"),
        F.initcap(F.trim("channel")).alias("channel_title"),
        F.coalesce("channel", F.lit("OTHER")).alias("channel_clean"),
    )
)

metrics = (
    filtered
    .select(
        "*",
        F.sum("discount_pct").over(w).alias("running_discount_pct"),
        F.avg("discount_pct").over(w2).alias("discount_pct_avg_2_prior"),
        F.sum("discount_pct").over(w3).alias("running_discount_pct_2"),
    )
)

matched = (
    metrics
    .alias("s")
    .join(df_promotions.alias("pr"), (F.coalesce(F.col("s.channel"), F.lit("web")) == F.col("pr.channel")) & (F.col("s.ends_on") >= F.col("pr.starts_on")) & (F.col("s.ends_on") <= F.date_add(F.col("pr.ends_on"), 3)), "inner")
    .select(
        "s.*",
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
        "promo_code",
        F.col("promotion_discount_pct").alias("PROMOTION_DISCOUNT_PCT"),
        F.col("discount_pct").alias("DISCOUNT_PCT_VAL"),
    )
    .unpivot(
        ["id", "promo_id", "promo_code"],
        ["PROMOTION_DISCOUNT_PCT", "DISCOUNT_PCT_VAL"],
        "metric_name",
        "value",
    )
    .filter(F.col("value").isNotNull())
)
