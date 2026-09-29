from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("created_at").cast("bigint")).rangeBetween(-1800, Window.currentRow)
w2 = Window.orderBy(F.col("created_at_hour_minus_tag_length").desc_nulls_first())
w3 = Window.orderBy(F.col("created_at_hour_minus_tag_length").asc_nulls_first())

lines = df_carts.select("id", "created_at", F.explode_outer("coupon_codes").alias("tag"))

filtered = (
    lines
    .filter(F.col("tag").isNotNull() & (F.col("tag") != "WELCOME5"))
    .withColumn("tag_title", F.initcap(F.trim("tag")))
    .withColumn("created_at_hour", F.hour("created_at"))
    .withColumn("resolved_tag", F.coalesce("tag", F.lit("NONE")))
)

cleaned = (
    filtered
    .filter((F.col("created_at") > F.lit("2025-03-02 04:00:00").cast("timestamp")) & (~F.col("tag").eqNullSafe(F.lit("BUNDLE"))))
    .select(
        "id",
        "created_at",
        "tag_title",
        "created_at_hour",
        "resolved_tag",
        F.length("tag").alias("tag_length"),
    )
)

filtered_carts = (
    cleaned
    .filter(F.col("tag_length") >= 25)
    .withColumn("greatest_created_at_hour_tag_length", F.greatest("created_at_hour", "tag_length"))
    .withColumn("created_at_hour_minus_tag_length", (F.col("created_at_hour") - F.col("tag_length")))
)

scored = (
    filtered_carts
    .filter(F.col("created_at_hour") > 68)
    .withColumn("tag_length_count_30m", F.count("tag_length").over(w))
    .withColumn("created_at_hour_minus_tag_length_cume_dist", F.cume_dist().over(w2))
    .withColumn("created_at_hour_minus_tag_length_dense_rank", F.dense_rank().over(w3))
)

filtered_carts_2 = (
    scored
    .filter(F.col("created_at_hour_minus_tag_length") > 35)
    .withColumn("tag_length_count_30m_rounded", F.round("tag_length_count_30m", 2))
)

matched_keys = (
    filtered_carts_2
    .select("id")
    .intersect(
        df_carts
        .filter(F.col("customer_id") == 12)
        .select("id")
    )
)

verified = (
    filtered_carts_2
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    verified
    .withColumn("tag_length_count_30m_tier", F.when(F.col("tag_length_count_30m") >= 14, "PREMIUM").otherwise("STANDARD"))
    .withColumn("created_at_hour_minus_tag_length_cume_dist_tier", F.when(F.col("created_at_hour_minus_tag_length_cume_dist") >= 0.87, "HIGH").otherwise("LOW"))
    .withColumn("activity_day", F.date_trunc("DAY", "created_at"))
    .groupingSets(
        [["tag_length_count_30m_tier", "created_at_hour_minus_tag_length_cume_dist_tier", "activity_day"], ["tag_length_count_30m_tier", "created_at_hour_minus_tag_length_cume_dist_tier"], []],
        "tag_length_count_30m_tier",
        "created_at_hour_minus_tag_length_cume_dist_tier",
        "activity_day",
    )
    .agg(
        F.round(F.avg("created_at_hour_minus_tag_length"), 2).alias("avg_created_at_hour_minus_tag_length"),
        F.max("created_at").alias("last_created_at"),
    )
    .filter(F.col("avg_created_at_hour_minus_tag_length") >= 0)
)
