from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo_code").orderBy(F.col("discount_pct").desc())
w2 = Window.partitionBy("channel").orderBy("discount_pct", "id").rowsBetween(-3, Window.currentRow)
w3 = Window.partitionBy("promo_code", "channel").orderBy(F.col("discount_pct_avg_3").desc(), "id")
w4 = Window.partitionBy("promo_code").orderBy("ends_on", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("promotion_promo_code").orderBy("discount_pct", "id", "promo_id")
w6 = Window.partitionBy("promotion_promo_code", "channel").orderBy(F.col("discount_pct_avg_3").desc_nulls_last(), "id", "promo_id", "promo_id_2")

enriched = (
    df_promotions
    .withColumn("discount_pct_percent_rank", F.percent_rank().over(w))
    .withColumn("discount_pct_avg_3", F.avg("discount_pct").over(w2))
)

scored = (
    enriched
    .select(
        "*",
        F.row_number().over(w3).alias("row_seq"),
        F.sum("discount_pct_avg_3").over(w4).alias("cumulative_discount_pct_avg_3"),
    )
)

with_window = (
    scored
    .alias("a")
    .join(df_promotions.alias("pr"), (F.col("a.channel").eqNullSafe(F.col("pr.channel"))) & (F.col("a.ends_on") >= F.col("pr.starts_on")) & (F.col("a.ends_on") <= F.date_add(F.col("pr.ends_on"), 7)), "left")
    .select(
        "a.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code").alias("promotion_promo_code"),
        F.col("pr.discount_pct").alias("promotion_discount_pct"),
    )
)

ranked = (
    with_window
    .withColumn("latest_rank", F.row_number().over(w5))
    .filter(F.col("latest_rank") == 1)
    .drop("latest_rank")
)

base = (
    ranked
    .filter((F.col("ends_on") >= F.lit("2024-02-08").cast("date")) & (F.col("promotion_promo_code") == "VIP"))
    .withColumn("discount_pct_level", F.when(F.col("discount_pct") >= 0.27, "High").when(F.col("discount_pct") >= 0.14, "Medium").otherwise("Low"))
    .withColumn("channel_filled", F.coalesce("channel", F.lit("Unknown")))
    .withColumn("promotion_promo_code_filled", F.coalesce("promotion_promo_code", F.lit("UNKNOWN")))
)

matched = (
    base
    .alias("s")
    .join(df_promotions.alias("pr"), (F.col("s.channel") == F.col("pr.channel")) & (F.col("s.starts_on") >= F.col("pr.starts_on")) & (F.col("s.starts_on") <= F.date_add(F.col("pr.ends_on"), 7)), "left")
    .select(
        "s.*",
        F.col("pr.id").alias("promo_id_2"),
        F.col("pr.promo_code").alias("promotion_promo_code_2"),
        F.col("pr.discount_pct").alias("promotion_discount_pct_2"),
    )
)

deduped = (
    matched
    .withColumn("latest_rank", F.row_number().over(w6))
    .filter(F.col("latest_rank") == 1)
    .drop("latest_rank")
)

pivoted = (
    deduped
    .groupBy("promotion_promo_code_2", "channel")
    .pivot("promo_code", ["VIP", "WELCOME5", "SPRING10"])
    .agg(F.sum("promo_id"))
)

result = (
    pivoted
    .select("promotion_promo_code_2", "channel", "VIP", "SPRING10")
    .orderBy(
        F.col("channel").desc_nulls_first(),
        F.col("promotion_promo_code_2").desc(),
    )
)
