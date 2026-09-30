from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel").orderBy(F.col("discount_pct").desc(), "id")
w2 = Window.partitionBy("promo_code").orderBy("dedup_rank", "id")
w_ordered = Window.partitionBy("promo_code", "channel").orderBy("starts_on_plus_months", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("promo_code").orderBy("promo_id", "id", "promo_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("promo_code", "channel")

filtered = (
    df_promotions
    .filter(F.col("ends_on") >= F.lit("2023-11-29").cast("date"))
    .select(
        "*",
        F.lower("channel").alias("channel_lower"),
        F.date_format("starts_on", "MMM yyyy").alias("starts_on_label"),
    )
)

ranked = (
    filtered
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
)

ranked_promotions = (
    ranked
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select(
        "id",
        "channel",
        "promo_code",
        "starts_on",
        "ends_on",
        "channel_lower",
        "starts_on_label",
        "dedup_rank",
    )
)

cleaned = (
    ranked_promotions
    .filter(F.col("starts_on") >= F.to_date(F.lit("2023-07-12")))
    .select(
        "*",
        F.add_months("starts_on", 12).alias("starts_on_plus_months"),
        F.concat("promo_code", F.lit(" - "), "channel").alias("promo_code_channel"),
        F.coalesce("channel", F.lit("OTHER")).alias("resolved_channel"),
    )
)

with_windows = (
    cleaned
    .withColumn("last_known_channel", F.last("channel", ignorenulls=True).over(w_ordered))
)

matched = (
    with_windows
    .alias("a")
    .join(df_promotions.alias("pr"), (F.col("a.channel").eqNullSafe(F.col("pr.channel"))) & (F.col("a.starts_on_plus_months") >= F.col("pr.starts_on")) & (F.col("a.starts_on_plus_months") < F.col("pr.ends_on") + F.expr("INTERVAL 30 DAYS")), "left")
    .select(
        "a.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code").alias("promotion_promo_code"),
        F.col("pr.discount_pct"),
    )
)

with_windows_promotions = (
    matched
    .select(
        "*",
        F.first("promo_id", ignorenulls=True).over(w4).alias("first_known_promo_id"),
        F.max("promo_id").over(w5).alias("promo_code_max_promo_id"),
    )
)

result = (
    with_windows_promotions
    .groupBy("promotion_promo_code", "promo_code_max_promo_id")
    .agg(
        F.sort_array(F.collect_set("promotion_promo_code")).alias("promotion_promo_code_list"),
        F.sum("promo_id").alias("total_promo_id"),
        F.count(F.when(F.col("channel") == "store", 1)).alias("store_count"),
        F.size(F.collect_set("promotion_promo_code")).alias("num_promotion_promo_codes"),
        F.percentile_approx("promo_id", 0.95).alias("promo_id_p95"),
    )
)
