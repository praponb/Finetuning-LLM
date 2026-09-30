from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("is_spring10", "id").rowsBetween(-2, -1)
w2 = Window.orderBy(F.col("discount_pct_rounded").desc(), "id")
w3 = Window.partitionBy("promo_code", "channel")
w4 = Window.partitionBy("promo_code").orderBy("starts_on", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("promo_code").orderBy(F.col("discount_pct").desc(), "id")

base = (
    df_promotions
    .filter((F.col("starts_on") >= F.to_date(F.lit("2023-09-29"))) & F.col("channel").isNotNull())
    .select(
        "*",
        F.round("discount_pct", 0).alias("discount_pct_rounded"),
        F.when(F.col("promo_code") == "SPRING10", 1).otherwise(0).alias("is_spring10"),
        F.coalesce("channel", F.lit("N/A")).alias("resolved_channel"),
    )
)

with_windows = (
    base
    .select(
        "*",
        F.avg("is_spring10").over(w).alias("is_spring10_avg_2_prior"),
        F.ntile(5).over(w2).alias("discount_pct_rounded_bucket"),
        F.round((F.col("discount_pct") / F.sum("discount_pct").over(w3)), 4).alias("discount_pct_share"),
        F.first("discount_pct_rounded", ignorenulls=True).over(w4).alias("first_known_discount_pct_rounded"),
    )
)

base_promotions = (
    with_windows
    .filter((F.col("starts_on") >= F.lit("2024-03-06").cast("date")) & F.col("channel").isNotNull())
    .select(
        "id",
        "promo_code",
        "starts_on",
        "ends_on",
        "discount_pct",
        "discount_pct_rounded",
        "resolved_channel",
        "is_spring10_avg_2_prior",
        "first_known_discount_pct_rounded",
        F.col("discount_pct").cast("int").alias("discount_pct_int"),
        (F.col("first_known_discount_pct_rounded") - F.col("discount_pct_rounded_bucket")).alias("first_known_discount_pct_rounded_minus_discount_pct_rounded_bucket"),
        F.coalesce("channel", F.lit("OTHER")).alias("channel_filled"),
    )
)

deduped = (
    base_promotions
    .withColumn("row_num", F.row_number().over(w5))
    .filter(F.col("row_num") == 1)
)

result = (
    deduped
    .select(
        "id",
        "promo_code",
        "starts_on",
        "discount_pct",
        "discount_pct_rounded",
        "is_spring10_avg_2_prior",
        "discount_pct_int",
        "first_known_discount_pct_rounded_minus_discount_pct_rounded_bucket",
        "channel_filled",
    )
    .orderBy("discount_pct_int")
)
