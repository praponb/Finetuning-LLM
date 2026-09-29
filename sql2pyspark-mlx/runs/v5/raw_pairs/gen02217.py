from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo_code", "channel").orderBy("starts_on", "id")
w2 = Window.partitionBy("channel").orderBy("discount_pct")
w_hist = Window.partitionBy("channel").orderBy("discount_pct", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("channel").orderBy("discount_pct", "id")
w_grp = Window.partitionBy("channel")
w6 = Window.orderBy("discount_pct", "id").rowsBetween(-2, Window.currentRow)
w7 = Window.partitionBy("channel").orderBy("cumulative_discount_pct", "id")
w8 = Window.partitionBy("channel").orderBy("discount_pct", "id").rowsBetween(-5, Window.currentRow)
w9 = Window.partitionBy("channel").orderBy(F.col("discount_pct").desc(), "id")
w10 = Window.partitionBy("channel").orderBy("channel_min_discount_pct_share", "id")

deduped = (
    df_promotions
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("id", "channel", "discount_pct")
)

ranked = (
    deduped
    .withColumn("rn", F.rank().over(w2))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

scored = (
    ranked
    .filter(F.col("discount_pct") >= 0.32)
    .withColumn("cumulative_discount_pct", F.sum("discount_pct").over(w_hist))
    .withColumn("discount_pct_bucket", F.ntile(10).over(w4))
    .withColumn("channel_min_discount_pct", F.min("discount_pct").over(w_grp))
    .withColumn("first_known_channel", F.first("channel", ignorenulls=True).over(w_hist))
    .withColumn("last_known_channel", F.last("channel", ignorenulls=True).over(w_hist))
)

with_windows = (
    scored
    .withColumn("channel_min_discount_pct_share", F.round((F.col("channel_min_discount_pct") / F.sum("channel_min_discount_pct").over(w_grp)), 4))
    .withColumn("discount_pct_bucket_avg_2", F.avg("discount_pct_bucket").over(w6))
    .withColumn("cumulative_discount_pct_bucket", F.ntile(4).over(w7))
    .withColumn("discount_pct_bucket_avg_5", F.avg("discount_pct_bucket").over(w8))
    .withColumn("event_seq", F.row_number().over(w9))
)

deduped_promotions = (
    with_windows
    .withColumn("rn", F.row_number().over(w10))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

filtered = (
    deduped_promotions
    .filter(F.col("discount_pct_bucket_avg_5") == 4)
    .select(
        "*",
        (F.col("discount_pct_bucket") * F.col("channel_min_discount_pct_share")).alias("discount_pct_bucket_times_channel_min_discount_pct_share"),
        F.col("discount_pct_bucket").cast("string").alias("discount_pct_bucket_str"),
        F.coalesce("channel", F.lit("NONE")).alias("resolved_channel"),
    )
)

result = (
    filtered
    .groupBy("channel")
    .agg(
        F.count_if(F.col("channel") == "partner").alias("partner_events"),
        F.percentile_approx("discount_pct_bucket_avg_5", [0.25, 0.5, 0.75]).alias("discount_pct_bucket_avg_5_quartiles"),
        F.count(F.when(F.col("channel") == "partner", 1)).alias("partner_count"),
        F.round(F.sum("channel_min_discount_pct_share") / F.nullif(F.count(F.when(F.col("channel") == "mobile", 1)), F.lit(0)), 2).alias("channel_min_discount_pct_share_per_mobile"),
        F.percentile_approx("channel_min_discount_pct", [0.25, 0.5, 0.75]).alias("channel_min_discount_pct_quartiles"),
    )
    .orderBy("channel")
)
