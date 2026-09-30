from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("ends_on", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("promo_code").orderBy("discount_pct", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("promo_code").orderBy("ends_on", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("promo_code").orderBy("ends_on", "id").rowsBetween(-2, -1)
w5 = Window.partitionBy("channel").orderBy(F.col("discount_pct").desc(), "id")
w6 = Window.partitionBy("channel", "promo_code").orderBy("starts_on", "id")
w7 = Window.partitionBy("channel")
w8 = Window.partitionBy("promo_code").orderBy("discount_pct", "id").rowsBetween(-6, Window.currentRow)
w9 = Window.orderBy("event_seq", "id", "promo_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w10 = Window.partitionBy("channel").orderBy("discount_pct_bucket", "id", "promo_id")

scored = (
    df_promotions
    .withColumn("running_discount_pct", F.sum("discount_pct").over(w))
    .withColumn("running_discount_pct_2", F.sum("discount_pct").over(w2))
    .withColumn("cumulative_discount_pct", F.sum("discount_pct").over(w3))
    .withColumn("discount_pct_avg_2_prior", F.avg("discount_pct").over(w4))
    .withColumn("discount_pct_bucket", F.ntile(5).over(w5))
)

metrics = (
    scored
    .withColumn("next_discount_pct_avg_2_prior", F.lead("discount_pct_avg_2_prior").over(w6))
    .withColumn("next_cumulative_discount_pct", F.lead("cumulative_discount_pct").over(w6))
    .withColumn("prev_cumulative_discount_pct", F.lag("cumulative_discount_pct").over(w6))
    .withColumn("event_seq", F.row_number().over(w6))
)

enriched = (
    metrics
    .withColumn("discount_pct_bucket_share", F.round((F.col("discount_pct_bucket") / F.sum("discount_pct_bucket").over(w7)), 4))
    .withColumn("prev_cumulative_discount_pct_share", F.round((F.col("prev_cumulative_discount_pct") / F.sum("prev_cumulative_discount_pct").over(w7)), 4))
    .withColumn("running_discount_pct_2_avg_6", F.avg("running_discount_pct_2").over(w8))
)

valid = (
    enriched
    .filter(F.col("starts_on") > F.lit("2023-04-09").cast("date"))
    .select(
        "id",
        "channel",
        "promo_code",
        "starts_on",
        "discount_pct",
        "running_discount_pct",
        "running_discount_pct_2",
        "cumulative_discount_pct",
        "discount_pct_bucket",
        "prev_cumulative_discount_pct",
        "event_seq",
        "discount_pct_bucket_share",
        "prev_cumulative_discount_pct_share",
        (F.col("prev_cumulative_discount_pct") + F.col("discount_pct")).alias("prev_cumulative_discount_pct_plus_discount_pct"),
    )
)

matched = (
    valid
    .alias("a")
    .join(df_promotions.alias("pr"), (F.col("a.channel").eqNullSafe(F.col("pr.channel"))) & F.col("a.starts_on").between(F.col("pr.starts_on"), F.col("pr.ends_on")), "inner")
    .select(
        "a.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code").alias("promotion_promo_code"),
        F.col("pr.discount_pct").alias("promotion_discount_pct"),
    )
)

metrics_promotions = (
    matched
    .withColumn("cumulative_event_seq", F.sum("event_seq").over(w9))
    .withColumn("next_prev_cumulative_discount_pct", F.lead("prev_cumulative_discount_pct", 2).over(w10))
)

summary = (
    metrics_promotions
    .withColumn("activity_month", F.date_trunc("MONTH", "starts_on"))
    .groupingSets(
        [["promo_code", "activity_month"], ["promo_code"], []],
        "promo_code",
        "activity_month",
    )
    .agg(
        F.count(F.when(F.col("promo_code") == "SPRING10", 1)).alias("spring10_rows"),
        F.count(F.when(F.col("promotion_promo_code") == "VIP", 1)).alias("vip_count"),
        F.count(F.when(F.col("promo_code") == "WELCOME5", 1)).alias("welcome5_count"),
    )
)

result = (
    summary
    .select("promo_code", "welcome5_count")
    .orderBy(F.col("promo_code").desc_nulls_last())
)
