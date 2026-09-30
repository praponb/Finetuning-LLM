from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo_code", "channel").orderBy("starts_on", "id")
w2 = Window.partitionBy("channel").orderBy("dedup_rank", "id")
w3 = Window.partitionBy("promo_code").orderBy("starts_on", "id").rowsBetween(-5, Window.currentRow)
w4 = Window.partitionBy("channel").orderBy("discount_pct", "id").rowsBetween(-2, -1)
w5 = Window.partitionBy("promo_code", "channel")
w6 = Window.partitionBy("channel").orderBy(F.col("next_discount_pct").desc(), "id")

cleaned = (
    df_promotions
    .filter(F.col("channel").isNotNull() & (F.col("starts_on") >= F.lit("2023-10-08").cast("date")))
    .select(
        "id",
        "channel",
        "promo_code",
        "starts_on",
        "ends_on",
        "discount_pct",
        F.trim("promo_code").alias("clean_promo_code"),
        F.when(F.col("discount_pct") >= 0.3, "High").when(F.col("discount_pct") >= 0.16, "Medium").otherwise("Low").alias("discount_pct_level"),
        F.coalesce("channel", F.lit("N/A")).alias("resolved_channel"),
    )
)

ranked = (
    cleaned
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
)

enriched = (
    ranked
    .withColumn("next_discount_pct", F.lead("discount_pct").over(w2))
    .withColumn("row_seq", F.row_number().over(w2))
)

with_windows = (
    enriched
    .filter(F.col("next_discount_pct") <= 0.25)
    .select(
        "id",
        "channel",
        "promo_code",
        "discount_pct",
        "resolved_channel",
        "dedup_rank",
        "next_discount_pct",
        "row_seq",
        F.avg("discount_pct").over(w3).alias("discount_pct_avg_5"),
        F.avg("row_seq").over(w4).alias("row_seq_avg_2_prior"),
        F.avg("discount_pct").over(w5).alias("promo_code_avg_discount_pct"),
        F.lead("row_seq", 1).over(w2).alias("next_row_seq"),
    )
)

deduped = (
    with_windows
    .withColumn("dedup_rank_2", F.row_number().over(w6))
    .filter(F.col("dedup_rank_2") == 1)
    .drop("dedup_rank_2")
)

result = (
    deduped
    .groupBy(
        F.when(F.col("row_seq") >= 9, "HIGH").otherwise("LOW").alias("row_seq_tier"),
    )
    .agg(
        F.round(F.avg("promo_code_avg_discount_pct"), 2).alias("avg_promo_code_avg_discount_pct"),
        F.round(F.sum("discount_pct_avg_5") / F.nullif(F.count(F.when(F.col("promo_code") == "BUNDLE", 1)), F.lit(0)), 2).alias("discount_pct_avg_5_per_bundle"),
        F.size(F.collect_set("promo_code")).alias("num_promo_codes"),
    )
)
