from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo_code").orderBy("ends_on_period", "id")
w2 = Window.partitionBy("promo_code").orderBy("ends_on_period", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_recent = Window.orderBy(F.col("island_id").desc())
w_recent_2 = Window.partitionBy("promo_code").orderBy("ends_on_period", "id").rowsBetween(-2, -1)
w_hist = Window.partitionBy("promo_code").orderBy("island_id", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.orderBy("burst_id", "id")
w7 = Window.orderBy(F.col("ends_on_period").cast("bigint")).rangeBetween(-604800, Window.currentRow)
w_running = Window.orderBy("ends_on_period", "id").rowsBetween(-2, -1)
w9 = Window.orderBy("island_id_percent_rank", "id")
w10 = Window.orderBy("session_start_flag", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w11 = Window.orderBy("ends_on_period", "id")
w12 = Window.orderBy(F.col("ends_on_period").cast("bigint")).rangeBetween(-7776000, Window.currentRow)

cleaned = (
    df_promotions
    .filter(F.col("promo_code") != "VIP")
    .select(
        "id",
        "promo_code",
        "ends_on",
        F.lower("channel").alias("channel_lower"),
        F.date_trunc("WEEK", "ends_on").alias("ends_on_period"),
    )
)

gaps = (
    cleaned
    .withColumn("new_group_flag", F.when(F.col("ends_on_period") - F.lag("ends_on_period").over(w) > F.expr("INTERVAL 90 DAYS"), 1).otherwise(0))
)

sessionized = gaps.withColumn("island_id", F.sum("new_group_flag").over(w2))

flagged = (
    sessionized
    .withColumn("session_start_flag", F.when(F.lag("ends_on_period").over(w).isNull() | (F.col("ends_on_period") > F.lag("ends_on_period").over(w) + F.expr("INTERVAL 60 DAYS")), 1).otherwise(0))
)

islands = flagged.withColumn("burst_id", F.sum("session_start_flag").over(w2))

with_windows = (
    islands
    .select(
        "*",
        F.first("new_group_flag", ignorenulls=True).over(w2).alias("first_known_new_group_flag"),
        F.percent_rank().over(w_recent).alias("island_id_percent_rank"),
        F.avg("session_start_flag").over(w_recent_2).alias("session_start_flag_avg_2_prior"),
        F.last("new_group_flag", ignorenulls=True).over(w_hist).alias("last_known_new_group_flag"),
    )
)

filtered = (
    with_windows
    .filter(F.col("ends_on") > F.lit("2023-06-23").cast("date"))
    .select(
        "id",
        "ends_on",
        "channel_lower",
        "ends_on_period",
        "island_id",
        "session_start_flag",
        "burst_id",
        "first_known_new_group_flag",
        "island_id_percent_rank",
        F.last_day("ends_on").alias("ends_on_month_end"),
    )
)

scored = (
    filtered
    .select(
        "*",
        F.lead("first_known_new_group_flag", 1).over(w6).alias("next_first_known_new_group_flag"),
        F.max("first_known_new_group_flag").over(w7).alias("first_known_new_group_flag_max_7d"),
    )
)

with_windows_promotions = (
    scored
    .select(
        "*",
        F.avg("next_first_known_new_group_flag").over(w_running).alias("next_first_known_new_group_flag_avg_2_prior"),
        F.lead("island_id", 2, 0).over(w9).alias("next_island_id"),
        F.sum("island_id_percent_rank").over(w10).alias("cumulative_island_id_percent_rank"),
        F.lead("island_id", 1).over(w11).alias("next_island_id_2"),
        F.count("burst_id").over(w12).alias("burst_id_count_90d"),
    )
)

grouped = (
    with_windows_promotions
    .groupBy("island_id", "burst_id")
    .agg(
        F.percentile_approx("next_first_known_new_group_flag_avg_2_prior", [0.25, 0.5, 0.75]).alias("next_first_known_new_group_flag_avg_2_prior_quartiles"),
        ((F.unix_timestamp(F.max("ends_on_period")) - F.unix_timestamp(F.min("ends_on_period"))) / 60).alias("duration_min"),
        F.countDistinct("id").alias("distinct_ids"),
    )
)

result = (
    grouped
    .select(
        "island_id",
        "burst_id",
        "next_first_known_new_group_flag_avg_2_prior_quartiles",
        "duration_min",
        "distinct_ids",
    )
    .orderBy(F.col("burst_id").asc_nulls_first(), F.col("distinct_ids").desc())
)
