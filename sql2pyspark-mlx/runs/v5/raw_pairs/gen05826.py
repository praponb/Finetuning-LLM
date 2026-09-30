from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.partitionBy("promo_code").orderBy("discount_pct", "id").rowsBetween(-3, Window.currentRow)
w2 = Window.partitionBy("promo_code").orderBy(F.col("discount_pct").desc(), "id")
w3 = Window.partitionBy("promo_code").orderBy(F.col("last4_avg_discount_pct").desc(), "id")

filtered = (
    df_promotions
    .filter((F.col("starts_on") >= F.to_date(F.lit("2023-09-14"))) & F.col("channel").isNotNull())
    .select(
        "id",
        "promo_code",
        "starts_on",
        "ends_on",
        "discount_pct",
        F.date_format("ends_on", "MMM yyyy").alias("ends_on_label"),
        F.lower("channel").alias("channel_lower"),
        F.coalesce("channel", F.lit("DIRECT")).alias("resolved_channel"),
    )
)

enriched = (
    filtered
    .filter(F.col("discount_pct") > 0.32)
    .select(
        "id",
        "promo_code",
        "ends_on",
        "discount_pct",
        "ends_on_label",
        "channel_lower",
        F.min("discount_pct").over(w_ordered).alias("last4_min_discount_pct"),
        F.max("discount_pct").over(w_ordered).alias("last4_max_discount_pct"),
        F.avg("discount_pct").over(w_ordered).alias("last4_avg_discount_pct"),
        F.row_number().over(w2).alias("event_seq"),
    )
)

ranked = (
    enriched
    .withColumn("dedup_rank", F.row_number().over(w3))
    .filter(F.col("dedup_rank") == 1)
    .select(
        "id",
        "promo_code",
        "discount_pct",
        "ends_on_label",
        "channel_lower",
        "last4_max_discount_pct",
        "last4_avg_discount_pct",
        "event_seq",
    )
)

deduped = (
    ranked
    .withColumn("row_num", F.row_number().over(w3))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

grouped = (
    deduped
    .groupBy(
        F.when(F.col("last4_max_discount_pct") >= 0.33, "LARGE").otherwise("SMALL").alias("last4_max_discount_pct_tier"),
    )
    .agg(
        F.count(F.when(F.col("promo_code") == "WELCOME5", 1)).alias("welcome5_count"),
        F.sum(F.when(F.col("promo_code") == "SPRING10", F.col("last4_max_discount_pct")).otherwise(0)).alias("spring10_last4_max_discount_pct"),
        F.avg(F.when(F.col("promo_code") == "WELCOME5", F.col("last4_max_discount_pct"))).alias("welcome5_avg_last4_max_discount_pct"),
        F.round(F.sum("last4_max_discount_pct") / F.nullif(F.count(F.when(F.col("promo_code") == "SPRING10", 1)), F.lit(0)), 2).alias("last4_max_discount_pct_per_spring10"),
    )
)

result = (
    grouped
    .select(
        "last4_max_discount_pct_tier",
        "welcome5_count",
        "spring10_last4_max_discount_pct",
        "last4_max_discount_pct_per_spring10",
    )
    .orderBy(F.col("spring10_last4_max_discount_pct").desc())
)
