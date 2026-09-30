from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo_code").orderBy("starts_on", "id").rowsBetween(-3, Window.currentRow)
w2 = Window.partitionBy("promo_code")
w3 = Window.partitionBy("channel").orderBy("last4_min_discount_pct", "id").rowsBetween(-3, Window.currentRow)
w4 = Window.partitionBy("channel", "promo_code").orderBy("discount_pct", "id")
w5 = Window.partitionBy("channel").orderBy(F.col("last4_count_discount_pct_share").desc())
w6 = Window.partitionBy("promo_code", "channel").orderBy("last4_avg_discount_pct_share", "id").rowsBetween(-3, Window.currentRow)
w7 = Window.partitionBy("promo_code").orderBy("row_seq", "id")
w8 = Window.partitionBy("promo_code").orderBy(F.col("starts_on").desc(), "id")

filtered = (
    df_promotions
    .filter(F.col("ends_on") >= F.lit("2023-10-05").cast("date"))
    .select(
        "*",
        F.concat_ws(",", "promo_code", "channel").alias("promo_code_channel_key"),
    )
)

scored = (
    filtered
    .filter(F.col("channel").isin(["web", "mobile", "store"]))
    .withColumn("last4_count_discount_pct", F.count("discount_pct").over(w))
    .withColumn("last4_min_discount_pct", F.min("discount_pct").over(w))
    .withColumn("last4_avg_discount_pct", F.avg("discount_pct").over(w))
    .withColumn("discount_pct_share", F.round((F.col("discount_pct") / F.sum("discount_pct").over(w2)), 4))
)

metrics = (
    scored
    .select(
        "*",
        F.count("discount_pct_share").over(w3).alias("last4_count_discount_pct_share"),
        F.max("discount_pct_share").over(w3).alias("last4_max_discount_pct_share"),
        F.min("discount_pct_share").over(w3).alias("last4_min_discount_pct_share"),
        F.avg("discount_pct_share").over(w3).alias("last4_avg_discount_pct_share"),
        F.row_number().over(w4).alias("row_seq"),
    )
)

enriched = (
    metrics
    .withColumn("last4_count_discount_pct_share_rank", F.rank().over(w5))
    .withColumn("row_seq_avg_3", F.avg("row_seq").over(w6))
    .withColumn("event_seq", F.row_number().over(w7))
)

latest = (
    enriched
    .withColumn("latest_rank", F.row_number().over(w8))
    .filter(F.col("latest_rank") == 1)
)

summary = (
    latest
    .groupBy(
        F.when(F.col("event_seq") >= 9, "LARGE").otherwise("SMALL").alias("event_seq_tier"),
    )
    .agg(
        F.sum(F.when(F.col("promo_code") == "SPRING10", F.col("event_seq"))).alias("spring10_total_event_seq"),
        F.avg(F.when(F.col("promo_code") == "BUNDLE", F.col("last4_min_discount_pct_share"))).alias("bundle_avg_last4_min_discount_pct_share"),
    )
)

result = (
    summary
    .select(
        "event_seq_tier",
        "spring10_total_event_seq",
        "bundle_avg_last4_min_discount_pct_share",
    )
    .orderBy(
        F.col("event_seq_tier").desc(),
        F.col("bundle_avg_last4_min_discount_pct_share").desc(),
    )
)
