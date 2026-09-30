from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo_code", "channel").orderBy("discount_pct_adjusted", "id")
w2 = Window.partitionBy("channel").orderBy("starts_on", "id").rowsBetween(-5, Window.currentRow)
w3 = Window.orderBy("starts_on", "id")
w4 = Window.partitionBy("promo_code").orderBy("discount_pct_adjusted_avg_5", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("promo_code").orderBy("discount_pct_adjusted_avg_5", "id")
w6 = Window.partitionBy("promo_code").orderBy(F.col("next_discount_pct_adjusted").asc_nulls_first())
w7 = Window.partitionBy("promo_code").orderBy(F.col("discount_pct_adjusted").desc(), "id")
w8 = Window.partitionBy("promo_code").orderBy(F.col("last_known_next_discount_pct_adjusted").desc_nulls_last(), "id", "promo_id")
w9 = Window.partitionBy("promotion_promo_code").orderBy("seq_no", "id", "promo_id")
w10 = Window.partitionBy("promo_code").orderBy("seq_no", "id", "promo_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_promotions
    .filter((F.col("starts_on") > F.to_date(F.lit("2023-04-11"))) & F.col("channel").isNotNull() & (F.col("ends_on") <= "2024-10-01"))
    .select(
        "id",
        "channel",
        "promo_code",
        "starts_on",
        "ends_on",
        "discount_pct",
        F.add_months("ends_on", 3).alias("ends_on_plus_months"),
        F.round((F.col("discount_pct") * 1.15), 2).alias("discount_pct_adjusted"),
    )
)

with_windows = (
    cleaned
    .withColumn("next_discount_pct_adjusted", F.lead("discount_pct_adjusted", 2).over(w))
    .withColumn("discount_pct_adjusted_avg_5", F.avg("discount_pct_adjusted").over(w2))
)

scored = (
    with_windows
    .select(
        "*",
        F.row_number().over(w3).alias("event_seq"),
        F.last("next_discount_pct_adjusted", ignorenulls=True).over(w4).alias("last_known_next_discount_pct_adjusted"),
        F.row_number().over(w5).alias("seq_no"),
        F.percent_rank().over(w6).alias("next_discount_pct_adjusted_percent_rank"),
    )
)

deduped = (
    scored
    .withColumn("rn", F.row_number().over(w7))
    .filter(F.col("rn") == 1)
)

attributed = (
    deduped
    .alias("a")
    .join(df_promotions.alias("pr"), (F.coalesce(F.col("a.channel"), F.lit("store")) == F.col("pr.channel")) & (F.col("a.ends_on_plus_months") >= F.col("pr.starts_on")) & (F.col("a.ends_on_plus_months") <= F.date_add(F.col("pr.ends_on"), 3)), "inner")
    .select(
        "a.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code").alias("promotion_promo_code"),
        F.col("pr.discount_pct").alias("promotion_discount_pct"),
    )
)

ranked = (
    attributed
    .withColumn("latest_rank", F.row_number().over(w8))
    .filter(F.col("latest_rank") == 1)
)

result = (
    ranked
    .withColumn("prev_seq_no", F.lag("seq_no", 1).over(w9))
    .withColumn("cumulative_last_known_next_discount_pct_adjusted", F.sum("last_known_next_discount_pct_adjusted").over(w10))
)
