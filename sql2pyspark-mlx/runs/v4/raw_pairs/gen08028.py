from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("discount_pct_whole", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("promo_code").orderBy("discount_pct_whole", "id").rowsBetween(-6, Window.currentRow)

valid = (
    df_promotions
    .filter(F.col("channel").isNotNull())
    .select(
        "id",
        "promo_code",
        "starts_on",
        "discount_pct",
        F.floor("discount_pct").alias("discount_pct_whole"),
    )
)

metrics = (
    valid
    .select(
        "*",
        F.last("discount_pct_whole", ignorenulls=True).over(w).alias("last_known_discount_pct_whole"),
        F.avg("discount_pct").over(w2).alias("discount_pct_avg_6"),
    )
)

base = (
    metrics
    .filter(F.col("starts_on") > F.lit("2024-01-13").cast("date"))
    .select(
        "*",
        F.when(F.col("discount_pct") >= 0.33, "High").when(F.col("discount_pct") >= 0.14, "Medium").otherwise("Low").alias("discount_pct_band"),
    )
)

valid_promotions = (
    base
    .filter(F.col("starts_on") > F.lit("2023-10-04").cast("date"))
    .select(
        "id",
        "starts_on",
        "discount_pct",
        "discount_pct_whole",
        "last_known_discount_pct_whole",
        "discount_pct_band",
        F.weekofyear("starts_on").alias("starts_on_weekofyear"),
    )
)

summary = (
    valid_promotions
    .groupBy(
        F.when(F.col("discount_pct_whole") >= 64, "LARGE").otherwise("SMALL").alias("discount_pct_whole_tier"),
        F.when(F.col("discount_pct") >= 0.3, "PREMIUM").otherwise("STANDARD").alias("discount_pct_tier"),
    )
    .agg(
        F.count("*").alias("num_rows"),
        F.percentile_approx("last_known_discount_pct_whole", 0.95).alias("last_known_discount_pct_whole_p95"),
        F.count("*").alias("event_count"),
    )
)

result = (
    summary
    .select(
        "discount_pct_whole_tier",
        "discount_pct_tier",
        "num_rows",
        "last_known_discount_pct_whole_p95",
        "event_count",
    )
    .orderBy(F.col("last_known_discount_pct_whole_p95").asc_nulls_last())
)
