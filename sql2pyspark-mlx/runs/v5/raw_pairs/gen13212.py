from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo_code", "channel").orderBy(F.col("least_discount_pct_whole_discount_pct").asc_nulls_first())
w2 = Window.partitionBy("promo_code", "channel").orderBy(F.col("discount_pct").desc())
w3 = Window.partitionBy("channel").orderBy("promo_id", "id", "promo_id").rowsBetween(-3, Window.currentRow)
w4 = Window.partitionBy("channel", "promo_code")
w5 = Window.partitionBy("promo_code").orderBy(F.col("discount_pct").desc(), "id", "promo_id")

cleaned = (
    df_promotions
    .filter(F.col("channel").isin(["partner", "mobile"]))
    .select(
        "id",
        "channel",
        "promo_code",
        "discount_pct",
        F.floor("discount_pct").alias("discount_pct_whole"),
        F.add_months("starts_on", 3).alias("starts_on_plus_months"),
    )
)

base = (
    cleaned
    .filter((~F.col("channel").eqNullSafe(F.lit("partner"))) & (F.col("discount_pct") > 0.2))
    .select(
        "id",
        "channel",
        "promo_code",
        "discount_pct",
        "discount_pct_whole",
        "starts_on_plus_months",
        F.least("discount_pct_whole", "discount_pct").alias("least_discount_pct_whole_discount_pct"),
    )
)

metrics = (
    base
    .select(
        "id",
        "channel",
        "starts_on_plus_months",
        "least_discount_pct_whole_discount_pct",
        F.percent_rank().over(w).alias("least_discount_pct_whole_discount_pct_percent_rank"),
        F.dense_rank().over(w2).alias("discount_pct_dense_rank"),
    )
)

with_window = (
    metrics
    .alias("src")
    .join(df_promotions.alias("pr"), (F.coalesce(F.col("src.channel"), F.lit("web")) == F.col("pr.channel")) & (F.col("src.starts_on_plus_months") >= F.col("pr.starts_on")) & (F.col("src.starts_on_plus_months") < F.col("pr.ends_on") + F.expr("INTERVAL 14 DAYS")), "inner")
    .select(
        "src.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code"),
        F.col("pr.discount_pct"),
    )
)

with_windows = (
    with_window
    .withColumn("last4_min_discount_pct_dense_rank", F.min("discount_pct_dense_rank").over(w3))
    .withColumn("last4_avg_discount_pct_dense_rank", F.avg("discount_pct_dense_rank").over(w3))
    .withColumn("last4_sum_discount_pct_dense_rank", F.sum("discount_pct_dense_rank").over(w3))
    .withColumn("promo_id_share", F.round((F.col("promo_id") / F.sum("promo_id").over(w4)), 4))
)

deduped = (
    with_windows
    .withColumn("dedup_rank", F.row_number().over(w5))
    .filter(F.col("dedup_rank") == 1)
)

matched = (
    deduped
    .alias("e")
    .join(df_promotions.alias("pr"), (F.col("e.channel") == F.col("pr.channel")) & (F.col("e.starts_on_plus_months") >= F.col("pr.starts_on")) & (F.col("e.starts_on_plus_months") <= F.date_add(F.col("pr.ends_on"), 10)), "inner")
    .select(
        "e.*",
        F.col("pr.id").alias("promo_id_2"),
        F.col("pr.promo_code").alias("promotion_promo_code"),
        F.col("pr.discount_pct").alias("promotion_discount_pct"),
    )
)

grouped = (
    matched
    .withColumn("promo_id_2_tier", F.when(F.col("promo_id_2") >= 9, "PREMIUM").otherwise("STANDARD"))
    .withColumn("quarter_start", F.date_trunc("QUARTER", "starts_on_plus_months"))
    .groupingSets(
        [["promo_id_2_tier", "quarter_start"], ["promo_id_2_tier"]],
        "promo_id_2_tier",
        "quarter_start",
    )
    .agg(
        F.round(F.sum("least_discount_pct_whole_discount_pct_percent_rank") / F.nullif(F.count(F.when(F.col("promo_code") == "SPRING10", 1)), F.lit(0)), 2).alias("least_discount_pct_whole_discount_pct_percent_rank_per_spring10"),
        F.round(F.sum("least_discount_pct_whole_discount_pct_percent_rank") / F.nullif(F.count(F.when(F.col("promo_code") == "SPRING10", 1)), F.lit(0)), 2).alias("least_discount_pct_whole_discount_pct_percent_rank_per_spring10_2"),
        F.avg(F.when(F.col("promo_code") == "SPRING10", F.col("last4_min_discount_pct_dense_rank"))).alias("spring10_avg_last4_min_discount_pct_dense_rank"),
        F.size(F.collect_set("promotion_promo_code")).alias("num_promotion_promo_codes"),
        F.count("*").alias("row_count"),
    )
)

result = (
    grouped
    .select(
        "promo_id_2_tier",
        "least_discount_pct_whole_discount_pct_percent_rank_per_spring10",
        "least_discount_pct_whole_discount_pct_percent_rank_per_spring10_2",
        "spring10_avg_last4_min_discount_pct_dense_rank",
        "num_promotion_promo_codes",
    )
    .orderBy(F.col("num_promotion_promo_codes").desc_nulls_first())
)
