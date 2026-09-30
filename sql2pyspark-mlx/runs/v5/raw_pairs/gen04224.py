from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo_code").orderBy("discount_pct", "id")
w2 = Window.partitionBy("channel", "promo_code").orderBy("ends_on", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("channel").orderBy(F.col("seq_no").asc_nulls_last(), "id")
w4 = Window.partitionBy("channel").orderBy("cumulative_discount_pct")
w5 = Window.orderBy(F.col("cumulative_discount_pct_times_channel_length").desc_nulls_first())
w6 = Window.partitionBy("channel")
w_hist = Window.partitionBy("channel").orderBy(F.col("cumulative_discount_pct_times_channel_length_percent_rank").desc_nulls_first())

with_windows = (
    df_promotions
    .select(
        "id",
        "channel",
        "ends_on",
        F.row_number().over(w).alias("seq_no"),
        F.sum("discount_pct").over(w2).alias("cumulative_discount_pct"),
    )
)

latest = (
    with_windows
    .withColumn("rank_in_group", F.row_number().over(w3))
    .filter(F.col("rank_in_group") == 1)
    .select("id", "channel", "cumulative_discount_pct")
)

kept_keys = (
    latest
    .select("id", "channel")
    .subtract(
        df_promotions
        .filter(F.col("starts_on").between("2024-01-27", "2024-08-18"))
        .select("id", "channel")
    )
)

reconciled = (
    latest
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.channel") == F.col("k.channel")), "inner")
    .select("s.*")
)

cleaned = (
    reconciled
    .filter(F.col("channel").isNotNull())
    .select("*", F.length("channel").alias("channel_length"))
)

filtered = (
    cleaned
    .filter(F.col("channel") == "web")
    .withColumn("cumulative_discount_pct_times_channel_length", (F.col("cumulative_discount_pct") * F.col("channel_length")))
    .withColumn("resolved_channel", F.coalesce("channel", F.lit("DIRECT")))
)

scored = (
    filtered
    .filter(F.col("cumulative_discount_pct") < 1)
    .withColumn("cumulative_discount_pct_rank", F.rank().over(w4))
    .withColumn("cumulative_discount_pct_times_channel_length_percent_rank", F.percent_rank().over(w5))
)

with_windows_promotions = (
    scored
    .select(
        "id",
        "channel_length",
        "cumulative_discount_pct_times_channel_length",
        "cumulative_discount_pct_rank",
        F.count("cumulative_discount_pct_times_channel_length").over(w6).alias("channel_count_cumulative_discount_pct_times_channel_length"),
        F.dense_rank().over(w_hist).alias("cumulative_discount_pct_times_channel_length_percent_rank_dense_rank"),
    )
)

summary = (
    with_windows_promotions
    .groupBy(
        F.when(F.col("cumulative_discount_pct_times_channel_length_percent_rank_dense_rank") >= 7, "HIGH").otherwise("LOW").alias("cumulative_discount_pct_times_channel_length_percent_rank_dense_rank_tier"),
        F.when(F.col("channel_count_cumulative_discount_pct_times_channel_length") >= 15, "LARGE").otherwise("SMALL").alias("channel_count_cumulative_discount_pct_times_channel_length_tier"),
    )
    .agg(
        F.count("*").alias("event_count"),
        F.percentile_approx("cumulative_discount_pct_times_channel_length", [0.25, 0.5, 0.75]).alias("cumulative_discount_pct_times_channel_length_quartiles"),
        F.countDistinct("id").alias("distinct_ids"),
    )
)

result = (
    summary
    .select(
        "cumulative_discount_pct_times_channel_length_percent_rank_dense_rank_tier",
        "channel_count_cumulative_discount_pct_times_channel_length_tier",
        "cumulative_discount_pct_times_channel_length_quartiles",
        "distinct_ids",
    )
    .orderBy(
        F.col("cumulative_discount_pct_times_channel_length_percent_rank_dense_rank_tier").desc_nulls_first(),
    )
)
