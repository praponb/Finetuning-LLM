from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("attr_key").orderBy(F.col("coupon_codes_count").asc_nulls_first())
w2 = Window.partitionBy("attr_key").orderBy(F.col("created_at").cast("bigint")).rangeBetween(-3600, Window.currentRow)

flattened = (
    df_carts
    .select(
        "id",
        "created_at",
        "coupon_codes",
        F.explode_outer("attributes").alias("attr_key", "attr_value"),
    )
)

cleaned = (
    flattened
    .filter((F.col("created_at") > F.lit("2025-03-01").cast("date")) & (~F.col("attr_key").eqNullSafe(F.lit("device"))))
    .select("*", F.sort_array("coupon_codes").alias("sorted_coupon_codes"))
)

filtered = (
    cleaned
    .filter((F.col("created_at") > F.lit("2025-03-02 03:00:00").cast("timestamp")) & F.col("attr_key").isNotNull())
    .withColumn("coupon_codes_count", F.size("coupon_codes"))
    .withColumn("created_at_label", F.date_format("created_at", "MMM yyyy"))
)

enriched = (
    filtered
    .select(
        "*",
        F.rank().over(w).alias("coupon_codes_count_rank"),
        F.sum("coupon_codes_count").over(w2).alias("coupon_codes_count_sum_1h"),
    )
)

result = (
    enriched
    .withColumn("coupon_codes_count_rank_tier", F.when(F.col("coupon_codes_count_rank") >= 9, "LARGE").otherwise("SMALL"))
    .groupingSets(
        [["attr_key", "coupon_codes_count_rank_tier"], ["attr_key"]],
        "attr_key",
        "coupon_codes_count_rank_tier",
    )
    .agg(
        F.count("*").alias("event_count"),
        F.percentile_approx("coupon_codes_count_sum_1h", 0.95).alias("coupon_codes_count_sum_1h_p95"),
        F.round(F.avg("coupon_codes_count_rank"), 2).alias("avg_coupon_codes_count_rank"),
        F.grouping("attr_key").alias("gid"),
    )
)
