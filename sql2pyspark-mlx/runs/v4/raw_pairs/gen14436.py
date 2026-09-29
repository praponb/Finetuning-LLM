from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel").orderBy(F.col("ends_on_month_end").desc(), "id")
w2 = Window.orderBy(F.col("starts_on_period").cast("bigint")).rangeBetween(-7776000, Window.currentRow)
w3 = Window.partitionBy("channel").orderBy("ends_on", "id").rowsBetween(-2, -1)
w4 = Window.partitionBy("channel").orderBy("discount_pct")
w5 = Window.partitionBy("channel").orderBy("discount_pct", "id")
w6 = Window.partitionBy("channel").orderBy("dedup_rank", "id")
w7 = Window.partitionBy("channel").orderBy(F.col("starts_on_period").cast("bigint")).rangeBetween(-7776000, Window.currentRow)
w8 = Window.partitionBy("channel")
w9 = Window.partitionBy("channel").orderBy("discount_pct", "id").rowsBetween(-2, -1)

cleaned = (
    df_promotions
    .filter(F.col("starts_on") >= "2025-07-31")
    .select(
        "id",
        "channel",
        "starts_on",
        "ends_on",
        "discount_pct",
        F.date_trunc("month", "starts_on").alias("starts_on_period"),
        F.last_day("ends_on").alias("ends_on_month_end"),
        F.coalesce("channel", F.lit("OTHER")).alias("resolved_channel"),
    )
)

deduped = (
    cleaned
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
)

metrics = (
    deduped
    .select(
        "*",
        F.max("discount_pct").over(w2).alias("discount_pct_max_90d"),
        F.avg("discount_pct").over(w3).alias("discount_pct_avg_2_prior"),
        F.rank().over(w4).alias("discount_pct_rank"),
        F.lead("discount_pct", 1).over(w5).alias("next_discount_pct"),
    )
)

scored = (
    metrics
    .select(
        "*",
        F.lag("discount_pct", 1).over(w6).alias("prev_discount_pct"),
        F.count("discount_pct_rank").over(w7).alias("discount_pct_rank_count_90d"),
    )
)

result = (
    scored
    .select(
        "*",
        F.round((F.col("discount_pct_max_90d") / F.sum("discount_pct_max_90d").over(w8)), 4).alias("discount_pct_max_90d_share"),
        F.sum("discount_pct").over(w8).alias("channel_sum_discount_pct"),
        F.avg("discount_pct_avg_2_prior").over(w9).alias("discount_pct_avg_2_prior_avg_2_prior"),
    )
)
