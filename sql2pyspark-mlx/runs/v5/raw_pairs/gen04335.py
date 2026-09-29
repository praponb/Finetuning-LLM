from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("promotion_discount_pct_2", "id", "promo_id", "promo_id_2").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("promo_code", "promotion_promo_code").orderBy("discount_pct_adjusted", "id", "promo_id", "promo_id_2")
w3 = Window.partitionBy("promo_code").orderBy("ends_on", "id", "promo_id", "promo_id_2").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.orderBy("days_ends_on_to_starts_on", "id", "promo_id", "promo_id_2")
w5 = Window.partitionBy("promotion_promo_code").orderBy("promo_id_2", "id", "promo_id", "promo_id_2")
w6 = Window.partitionBy("promotion_promo_code_2", "promo_code").orderBy("rn", "id", "promo_id", "promo_id_2")
w7 = Window.partitionBy("promotion_promo_code").orderBy("starts_on", "id", "promo_id", "promo_id_2").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_promotions
    .filter(F.col("channel").isNotNull())
    .select(
        "*",
        F.datediff("starts_on", "ends_on").alias("days_ends_on_to_starts_on"),
        F.round((F.col("discount_pct") * 1.07), 2).alias("discount_pct_adjusted"),
        F.coalesce("channel", F.lit("UNKNOWN")).alias("resolved_channel"),
    )
)

attributed = (
    cleaned
    .alias("e")
    .join(df_promotions.alias("pr"), (F.coalesce(F.col("e.channel"), F.lit("store")) == F.col("pr.channel")) & (F.col("e.ends_on") >= F.col("pr.starts_on")) & (F.col("e.ends_on") < F.col("pr.ends_on") + F.expr("INTERVAL 14 DAYS")), "inner")
    .select(
        "e.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code").alias("promotion_promo_code"),
        F.col("pr.discount_pct").alias("promotion_discount_pct"),
    )
)

matched = (
    attributed
    .alias("e")
    .join(df_promotions.alias("pr"), (F.col("e.channel").eqNullSafe(F.col("pr.channel"))) & (F.col("e.ends_on") >= F.col("pr.starts_on")) & (F.col("e.ends_on") < F.col("pr.ends_on") + F.expr("INTERVAL 7 DAYS")), "left")
    .select(
        "e.*",
        F.col("pr.id").alias("promo_id_2"),
        F.col("pr.promo_code").alias("promotion_promo_code_2"),
        F.col("pr.discount_pct").alias("promotion_discount_pct_2"),
    )
)

scored = (
    matched
    .select(
        "*",
        F.last("channel", ignorenulls=True).over(w).alias("last_known_channel"),
        F.row_number().over(w2).alias("seq_no"),
    )
)

metrics = (
    scored
    .withColumn("last_known_promotion_promo_code", F.last("promotion_promo_code", ignorenulls=True).over(w3))
    .withColumn("days_ends_on_to_starts_on_bucket", F.ntile(5).over(w4))
)

latest = (
    metrics
    .withColumn("rn", F.row_number().over(w5))
    .filter(F.col("rn") == 1)
)

metrics_promotions = (
    latest
    .filter(~F.col("promotion_promo_code").isin(["VIP", "WELCOME5", "SPRING10"]))
    .select(
        "id",
        "channel",
        "promo_code",
        "starts_on",
        "days_ends_on_to_starts_on",
        "resolved_channel",
        "promo_id",
        "promotion_discount_pct",
        "promo_id_2",
        "promotion_promo_code_2",
        "last_known_channel",
        "seq_no",
        "last_known_promotion_promo_code",
        "rn",
        F.lag("promo_id", 2).over(w6).alias("prev_promo_id"),
        F.sum("promotion_discount_pct_2").over(w7).alias("cumulative_promotion_discount_pct_2"),
    )
)

totals = (
    metrics_promotions
    .groupBy("channel")
    .agg(
        F.round(F.sum("prev_promo_id") / F.nullif(F.count(F.when(F.col("channel") == "partner", 1)), F.lit(0)), 2).alias("prev_promo_id_per_partner"),
        F.round(F.avg("promotion_discount_pct"), 2).alias("avg_promotion_discount_pct"),
        F.count("*").alias("cnt"),
        F.count("*").alias("event_count"),
        F.count_if(F.col("promotion_promo_code_2") == "SPRING10").alias("spring10_events"),
    )
)

result = (
    totals
    .select(
        "channel",
        "prev_promo_id_per_partner",
        "avg_promotion_discount_pct",
        "cnt",
        "event_count",
    )
    .orderBy(F.col("prev_promo_id_per_partner").desc_nulls_last())
)
