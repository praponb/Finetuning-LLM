from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("code").orderBy(F.col("created_at").cast("bigint")).rangeBetween(-900, Window.currentRow)
w2 = Window.partitionBy("code")
w3 = Window.partitionBy("code").orderBy(F.col("created_at_hour").asc_nulls_last())

flattened = df_carts.select("id", "created_at", F.explode("coupon_codes").alias("code"))

valid = (
    flattened
    .filter((F.col("created_at") > F.lit("2025-03-02").cast("date")) & (F.col("code") == "SPRING10"))
    .select(
        "*",
        F.hour("created_at").alias("created_at_hour"),
        F.coalesce("code", F.lit("N/A")).alias("resolved_code"),
    )
)

enriched = (
    valid
    .filter(F.col("created_at_hour") < 53)
    .select(
        "id",
        "code",
        "created_at_hour",
        "resolved_code",
        F.avg("created_at_hour").over(w).alias("created_at_hour_avg_15m"),
        F.avg("created_at_hour").over(w2).alias("code_avg_created_at_hour"),
        F.avg("created_at_hour").over(w).alias("created_at_hour_avg_15m_2"),
        F.dense_rank().over(w3).alias("created_at_hour_dense_rank"),
        F.max("created_at_hour").over(w).alias("created_at_hour_max_15m"),
    )
)

with_windows = (
    enriched
    .select(
        "*",
        F.max("created_at_hour_max_15m").over(w2).alias("code_max_created_at_hour_max_15m"),
    )
)

stats = (
    with_windows
    .withColumn("created_at_hour_avg_15m_2_tier", F.when(F.col("created_at_hour_avg_15m_2") >= 81, "HIGH").otherwise("LOW"))
    .cube("created_at_hour_avg_15m_2_tier", "code")
    .agg(
        F.sort_array(F.collect_set("code")).alias("code_list"),
        F.max("created_at_hour_avg_15m").alias("max_created_at_hour_avg_15m"),
        F.avg("created_at_hour").alias("avg_created_at_hour"),
        F.percentile_approx("created_at_hour", 0.95).alias("created_at_hour_p95"),
        F.countDistinct("id").alias("distinct_ids"),
        F.count_if(F.col("code") == "WELCOME5").alias("welcome5_events"),
        F.grouping("created_at_hour_avg_15m_2_tier").alias("gid"),
    )
)

result = (
    stats
    .select(
        "created_at_hour_avg_15m_2_tier",
        "code_list",
        "max_created_at_hour_avg_15m",
        "avg_created_at_hour",
        "created_at_hour_p95",
        "welcome5_events",
        "gid",
    )
    .orderBy("gid")
)
