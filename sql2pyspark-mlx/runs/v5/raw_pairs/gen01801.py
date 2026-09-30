from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel").orderBy("starts_on_period", "id")
w2 = Window.partitionBy("channel").orderBy("starts_on_period", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_promotions
    .filter(F.col("starts_on") > F.lit("2023-10-11").cast("date"))
    .select(
        "id",
        "channel",
        "promo_code",
        "starts_on",
        "ends_on",
        "discount_pct",
        F.date_trunc("month", "starts_on").alias("starts_on_period"),
    )
)

filtered = (
    cleaned
    .filter((F.col("ends_on") >= F.lit("2023-08-19").cast("date")) & F.col("channel").isNotNull())
    .select(
        "id",
        "channel",
        "starts_on",
        "ends_on",
        "discount_pct",
        "starts_on_period",
        F.last_day("starts_on").alias("starts_on_month_end"),
    )
)

gaps = (
    filtered
    .withColumn("is_new_burst", F.when(F.lag("starts_on_period").over(w).isNull() | (F.col("starts_on_period") > F.lag("starts_on_period").over(w) + F.expr("INTERVAL 90 DAYS")), 1).otherwise(0))
)

islands = gaps.withColumn("session_no", F.sum("is_new_burst").over(w2))

matched = (
    islands
    .alias("src")
    .join(df_promotions.alias("pr"), (F.col("src.channel") == F.col("pr.channel")) & (F.col("src.starts_on_month_end") >= F.col("pr.starts_on")) & (F.col("src.starts_on_month_end") < F.col("pr.ends_on") + F.expr("INTERVAL 7 DAYS")), "left")
    .select(
        "src.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code"),
        F.col("pr.discount_pct").alias("promotion_discount_pct"),
    )
)

result = (
    matched
    .groupBy("channel")
    .agg(
        F.max("starts_on_period").alias("last_starts_on_period"),
        F.min("starts_on_period").alias("first_seen"),
        F.round(F.sum("discount_pct") / F.nullif(F.count(F.when(F.col("promo_code") == "BUNDLE", 1)), F.lit(0)), 2).alias("discount_pct_per_bundle"),
    )
)
