from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo_code").orderBy("discount_pct", "id").rowsBetween(-3, Window.currentRow)

scored = (
    df_promotions
    .select(
        "*",
        F.avg("discount_pct").over(w).alias("last4_avg_discount_pct"),
        F.count("discount_pct").over(w).alias("last4_count_discount_pct"),
        F.sum("discount_pct").over(w).alias("last4_sum_discount_pct"),
        F.max("discount_pct").over(w).alias("last4_max_discount_pct"),
    )
)

base = (
    scored
    .filter((F.col("ends_on") >= F.to_date(F.lit("2023-09-11"))) & F.col("last4_avg_discount_pct").between(0.2, 0.3))
    .select(
        "id",
        "channel",
        "starts_on",
        "last4_avg_discount_pct",
        "last4_count_discount_pct",
        (F.col("last4_max_discount_pct") - F.col("last4_avg_discount_pct")).alias("last4_max_discount_pct_minus_last4_avg_discount_pct"),
        F.coalesce("channel", F.lit("OTHER")).alias("channel_filled"),
    )
)

base_promotions = (
    base
    .filter((F.col("starts_on") >= F.lit("2023-04-05").cast("date")) & F.col("channel").isNotNull())
    .select(
        "id",
        "starts_on",
        "last4_avg_discount_pct",
        "last4_count_discount_pct",
        "last4_max_discount_pct_minus_last4_avg_discount_pct",
        "channel_filled",
        (F.col("last4_count_discount_pct") * F.col("last4_avg_discount_pct")).alias("last4_count_discount_pct_times_last4_avg_discount_pct"),
    )
)

filtered = (
    base_promotions
    .filter((F.col("starts_on") >= F.to_date(F.lit("2023-06-15"))) & F.col("starts_on").between("2024-07-01", "2024-10-24"))
    .select(
        "id",
        "last4_avg_discount_pct",
        "last4_count_discount_pct",
        "last4_max_discount_pct_minus_last4_avg_discount_pct",
        "channel_filled",
        "last4_count_discount_pct_times_last4_avg_discount_pct",
        F.round("last4_max_discount_pct_minus_last4_avg_discount_pct", 0).alias("last4_max_discount_pct_minus_last4_avg_discount_pct_rounded"),
    )
)

cleaned = (
    filtered
    .filter(F.col("last4_count_discount_pct_times_last4_avg_discount_pct").between(35, 70))
    .select(
        "*",
        F.round("last4_count_discount_pct_times_last4_avg_discount_pct", 1).alias("last4_count_discount_pct_times_last4_avg_discount_pct_rounded"),
    )
)

valid = (
    cleaned
    .filter(F.col("last4_avg_discount_pct") >= 0.13)
    .select(
        "id",
        "last4_avg_discount_pct",
        "last4_count_discount_pct",
        "last4_max_discount_pct_minus_last4_avg_discount_pct",
        "last4_count_discount_pct_times_last4_avg_discount_pct",
        "last4_count_discount_pct_times_last4_avg_discount_pct_rounded",
        (F.col("last4_max_discount_pct_minus_last4_avg_discount_pct_rounded") + F.col("last4_count_discount_pct_times_last4_avg_discount_pct")).alias("last4_max_discount_pct_minus_last4_avg_discount_pct_rounded_plus_last4_count_discount_pct_times_last4_avg_discount_pct"),
    )
)

grouped = (
    valid
    .withColumn("last4_count_discount_pct_times_last4_avg_discount_pct_rounded_tier", F.when(F.col("last4_count_discount_pct_times_last4_avg_discount_pct_rounded") >= 87, "PREMIUM").otherwise("STANDARD"))
    .withColumn("last4_count_discount_pct_times_last4_avg_discount_pct_tier", F.when(F.col("last4_count_discount_pct_times_last4_avg_discount_pct") >= 65, "LARGE").otherwise("SMALL"))
    .groupingSets(
        [["last4_count_discount_pct_times_last4_avg_discount_pct_rounded_tier", "last4_count_discount_pct_times_last4_avg_discount_pct_tier"], ["last4_count_discount_pct_times_last4_avg_discount_pct_rounded_tier"]],
        "last4_count_discount_pct_times_last4_avg_discount_pct_rounded_tier",
        "last4_count_discount_pct_times_last4_avg_discount_pct_tier",
    )
    .agg(
        F.min("last4_count_discount_pct").alias("min_last4_count_discount_pct"),
        F.countDistinct("id").alias("distinct_ids"),
        F.countDistinct("id").alias("distinct_ids_2"),
        F.percentile_approx("last4_max_discount_pct_minus_last4_avg_discount_pct", [0.25, 0.5, 0.75]).alias("last4_max_discount_pct_minus_last4_avg_discount_pct_quartiles"),
        F.grouping("last4_count_discount_pct_times_last4_avg_discount_pct_rounded_tier").alias("gid"),
    )
)

result = (
    grouped
    .select(
        "last4_count_discount_pct_times_last4_avg_discount_pct_rounded_tier",
        "min_last4_count_discount_pct",
        "distinct_ids",
        "last4_max_discount_pct_minus_last4_avg_discount_pct_quartiles",
    )
    .orderBy("distinct_ids")
)
