from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("code").orderBy(F.col("created_at_hour").desc_nulls_last())
w2 = Window.orderBy(F.col("created_at").cast("bigint")).rangeBetween(-7200, Window.currentRow)
w3 = Window.orderBy(F.col("created_at").cast("bigint")).rangeBetween(-1800, Window.currentRow)
w4 = Window.orderBy(F.col("created_at").cast("bigint")).rangeBetween(-900, Window.currentRow)

flattened = df_carts.select("id", "created_at", F.explode("coupon_codes").alias("code"))

valid = (
    flattened
    .filter(F.col("created_at") >= F.lit("2025-03-01").cast("date"))
    .withColumn("code_prefix", F.substring("code", 1, 2))
)

cleaned = (
    valid
    .filter((F.col("created_at") > F.lit("2025-03-02 00:00:00").cast("timestamp")) & F.col("code").isNotNull())
    .withColumn("created_at_hour", F.hour("created_at"))
    .withColumn("code_upper", F.upper("code"))
)

matched_keys = (
    cleaned
    .select("id")
    .exceptAll(
        df_carts
        .filter(F.col("customer_id").isin([22, 26, 30]))
        .select("id")
    )
)

verified = (
    cleaned
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

metrics = (
    verified
    .select(
        "created_at",
        "code_prefix",
        "created_at_hour",
        F.dense_rank().over(w).alias("created_at_hour_dense_rank"),
    )
)

enriched = (
    metrics
    .filter(F.col("created_at_hour_dense_rank") > 4)
    .select(
        "*",
        F.max("created_at_hour").over(w2).alias("created_at_hour_max_2h"),
        F.avg("created_at_hour").over(w3).alias("created_at_hour_avg_30m"),
        F.count("created_at_hour_dense_rank").over(w4).alias("created_at_hour_dense_rank_count_15m"),
    )
)

filtered = (
    enriched
    .filter(F.col("created_at_hour_max_2h") < 47)
    .select(
        "created_at",
        "code_prefix",
        "created_at_hour",
        "created_at_hour_avg_30m",
        "created_at_hour_dense_rank_count_15m",
        F.least("created_at_hour_dense_rank_count_15m", "created_at_hour_max_2h").alias("least_created_at_hour_dense_rank_count_15m_created_at_hour_max_2h"),
    )
)

result = (
    filtered
    .rollup(
        F.when(F.col("created_at_hour") >= 80, "LARGE").otherwise("SMALL").alias("created_at_hour_tier"),
        F.date_trunc("HOUR", "created_at").alias("hour_window"),
    )
    .agg(
        F.percentile_approx("least_created_at_hour_dense_rank_count_15m_created_at_hour_max_2h", [0.25, 0.5, 0.75]).alias("least_created_at_hour_dense_rank_count_15m_created_at_hour_max_2h_quartiles"),
        F.max("created_at").alias("ended_at"),
        F.round(F.avg("created_at_hour_avg_30m"), 2).alias("avg_created_at_hour_avg_30m"),
        ((F.unix_timestamp(F.max("created_at")) - F.unix_timestamp(F.min("created_at"))) / 60).alias("span_minutes"),
        F.count("*").alias("event_count"),
        F.percentile_approx("created_at_hour", [0.25, 0.5, 0.75]).alias("created_at_hour_quartiles"),
    )
    .filter(F.col("event_count") > 1)
    .orderBy(F.col("event_count").desc_nulls_first())
)
