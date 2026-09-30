from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("value")
w2 = Window.partitionBy("k").orderBy(F.col("created_at_hour").desc_nulls_last())
w3 = Window.orderBy(F.col("created_at").cast("bigint")).rangeBetween(-1800, Window.currentRow)
w4 = Window.partitionBy("k")

exploded = (
    df_carts
    .select(
        "id",
        "created_at",
        "coupon_codes",
        F.explode_outer("attributes").alias("k", "attr_value"),
    )
)

lines = exploded.select("created_at", "k", F.explode("coupon_codes").alias("value"))

filtered = (
    lines
    .filter(F.col("k").isin(["gift", "weight_kg", "device"]))
    .withColumn("k_title", F.initcap(F.trim("k")))
    .withColumn("created_at_hour", F.hour("created_at"))
    .withColumn("k_clean", F.coalesce("k", F.lit("NONE")))
)

with_windows = (
    filtered
    .filter(F.to_date("created_at") < "2023-03-01")
    .select(
        "k",
        "k_title",
        "created_at_hour",
        F.avg("created_at_hour").over(w).alias("value_avg_created_at_hour"),
        F.avg("created_at_hour").over(w).alias("value_avg_created_at_hour_2"),
        F.cume_dist().over(w2).alias("created_at_hour_cume_dist"),
        F.count("created_at_hour").over(w3).alias("created_at_hour_count_30m"),
        F.avg("created_at_hour").over(w4).alias("k_avg_created_at_hour"),
    )
)

result = (
    with_windows
    .withColumn("created_at_hour_cume_dist_tier", F.when(F.col("created_at_hour_cume_dist") >= 0.81, "HIGH").otherwise("LOW"))
    .withColumn("created_at_hour_count_30m_tier", F.when(F.col("created_at_hour_count_30m") >= 17, "PREMIUM").otherwise("STANDARD"))
    .groupingSets(
        [["created_at_hour_cume_dist_tier", "created_at_hour_count_30m_tier"], ["created_at_hour_cume_dist_tier"]],
        "created_at_hour_cume_dist_tier",
        "created_at_hour_count_30m_tier",
    )
    .agg(
        F.size(F.collect_set("k")).alias("num_ks"),
        F.count("*").alias("event_count"),
        F.percentile_approx("created_at_hour_cume_dist", [0.25, 0.5, 0.75]).alias("created_at_hour_cume_dist_quartiles"),
        F.sort_array(F.collect_set("k")).alias("k_list"),
        F.sum(F.when(F.col("k") == "utm_source", F.col("created_at_hour")).otherwise(0)).alias("utm_source_created_at_hour"),
        F.count_if(F.col("k") == "utm_source").alias("utm_source_events"),
        F.grouping("created_at_hour_cume_dist_tier").alias("grouping_level"),
    )
    .orderBy(F.col("utm_source_events").desc())
)
