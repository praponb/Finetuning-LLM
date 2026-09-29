from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("tag").orderBy("is_welcome5")
w_recent = Window.partitionBy("tag")
w3 = Window.partitionBy("tag").orderBy(F.col("created_at").cast("bigint")).rangeBetween(-1800, Window.currentRow)
w4 = Window.orderBy(F.col("created_at").cast("bigint")).rangeBetween(-900, Window.currentRow)
w5 = Window.orderBy(F.col("is_welcome5_rank_avg_15m").desc())
w6 = Window.orderBy(F.col("created_at").cast("bigint")).rangeBetween(-3600, Window.currentRow)

flattened = df_carts.select("id", "created_at", F.explode("coupon_codes").alias("tag"))

valid = (
    flattened
    .filter((F.col("created_at") >= F.lit("2025-03-01").cast("date")) & F.col("tag").isin(["BUNDLE", "WELCOME5", "SPRING10"]))
    .select(
        "*",
        F.when(F.col("tag") == "WELCOME5", 1).otherwise(0).alias("is_welcome5"),
    )
)

scored = (
    valid
    .select(
        "*",
        F.rank().over(w).alias("is_welcome5_rank"),
        F.min("is_welcome5").over(w_recent).alias("tag_min_is_welcome5"),
        F.min("is_welcome5").over(w_recent).alias("tag_min_is_welcome5_2"),
        F.max("is_welcome5").over(w3).alias("is_welcome5_max_30m"),
    )
)

filtered = (
    scored
    .filter((F.col("created_at") >= F.to_timestamp(F.lit("2025-03-02 02:00:00"))) & F.col("tag").isNotNull())
    .select(
        "id",
        "created_at",
        "tag",
        "is_welcome5",
        "is_welcome5_rank",
        "tag_min_is_welcome5",
        "tag_min_is_welcome5_2",
        "is_welcome5_max_30m",
        (F.col("is_welcome5") + F.col("is_welcome5_rank")).alias("is_welcome5_plus_is_welcome5_rank"),
        F.coalesce("tag", F.lit("UNKNOWN")).alias("tag_clean"),
    )
)

metrics = (
    filtered
    .withColumn("is_welcome5_rank_avg_15m", F.avg("is_welcome5_rank").over(w4))
    .withColumn("is_welcome5_max_30m_sum_30m", F.sum("is_welcome5_max_30m").over(w3))
)

scored_carts = (
    metrics
    .select(
        "*",
        F.dense_rank().over(w5).alias("is_welcome5_rank_avg_15m_dense_rank"),
        F.count("is_welcome5_max_30m_sum_30m").over(w6).alias("is_welcome5_max_30m_sum_30m_count_1h"),
    )
)

valid_carts = (
    scored_carts
    .filter(F.col("is_welcome5_plus_is_welcome5_rank") < 27)
    .select(
        "id",
        "created_at",
        "is_welcome5",
        "tag_min_is_welcome5",
        "is_welcome5_plus_is_welcome5_rank",
        "is_welcome5_rank_avg_15m",
        "is_welcome5_max_30m_sum_30m",
        "is_welcome5_max_30m_sum_30m_count_1h",
        F.when(F.col("tag").isin(["WELCOME5", "BUNDLE"]), "Core").otherwise("Other").alias("tag_group"),
        F.coalesce("tag", F.lit("NONE")).alias("resolved_tag"),
    )
)

result = (
    valid_carts
    .withColumn("hour_window", F.date_trunc("hour", "created_at"))
    .withColumn("activity_day", F.date_trunc("day", "created_at"))
    .groupingSets(
        [["hour_window", "activity_day"], ["hour_window"], []],
        "hour_window",
        "activity_day",
    )
    .agg(
        F.percentile_approx("tag_min_is_welcome5", 0.9).alias("tag_min_is_welcome5_p90"),
        F.min("created_at").alias("first_seen"),
        F.percentile_approx("is_welcome5_rank_avg_15m", [0.25, 0.5, 0.75]).alias("is_welcome5_rank_avg_15m_quartiles"),
    )
    .filter(F.col("tag_min_is_welcome5_p90") > 0)
    .orderBy(F.col("tag_min_is_welcome5_p90").desc_nulls_last())
)
