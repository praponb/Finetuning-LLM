from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("element").orderBy(F.col("created_at_hour").asc_nulls_first())
w_recent = Window.partitionBy("element")
w3 = Window.partitionBy("element").orderBy(F.col("created_at_hour").asc_nulls_last())
w4 = Window.partitionBy("element").orderBy(F.col("created_at").cast("bigint")).rangeBetween(-7200, Window.currentRow)
w5 = Window.orderBy(F.col("element_sum_created_at_hour").desc_nulls_first())

exploded = df_carts.select("id", "created_at", F.explode("coupon_codes").alias("element"))

valid = (
    exploded
    .filter(F.col("created_at") > F.to_date(F.lit("2025-03-02")))
    .withColumn("created_at_hour", F.hour("created_at"))
)

scored = (
    valid
    .select(
        "*",
        F.percent_rank().over(w).alias("created_at_hour_percent_rank"),
        F.sum("created_at_hour").over(w_recent).alias("element_sum_created_at_hour"),
        F.cume_dist().over(w3).alias("created_at_hour_cume_dist"),
        F.avg("created_at_hour").over(w4).alias("created_at_hour_avg_2h"),
        F.stddev_samp("created_at_hour").over(w_recent).alias("element_stddev_samp_created_at_hour"),
    )
)

scored_carts = (
    scored
    .withColumn("element_sum_created_at_hour_percent_rank", F.percent_rank().over(w5))
    .withColumn("element_sum_created_at_hour_percent_rank_2", F.sum("created_at_hour_percent_rank").over(w_recent))
    .withColumn("element_stddev_samp_created_at_hour_percent_rank", F.stddev_samp("created_at_hour_percent_rank").over(w_recent))
)

cleaned = (
    scored_carts
    .filter((F.col("created_at") > F.lit("2025-03-02 16:00:00").cast("timestamp")) & F.col("element").isNotNull())
    .select(
        "*",
        F.coalesce("element_sum_created_at_hour_percent_rank_2", F.lit(0)).alias("element_sum_created_at_hour_percent_rank_2_filled"),
        F.coalesce("element", F.lit("NONE")).alias("resolved_element"),
    )
)

cleaned_carts = (
    cleaned
    .filter((F.col("created_at") > F.lit("2025-03-02").cast("date")) & F.col("element").isNotNull())
    .select(
        "created_at",
        "element",
        "created_at_hour",
        "created_at_hour_percent_rank",
        "element_sum_created_at_hour",
        "created_at_hour_cume_dist",
        "created_at_hour_avg_2h",
        "element_stddev_samp_created_at_hour",
        "element_sum_created_at_hour_percent_rank",
        "element_sum_created_at_hour_percent_rank_2",
        "element_stddev_samp_created_at_hour_percent_rank",
        "element_sum_created_at_hour_percent_rank_2_filled",
        "resolved_element",
        F.least("created_at_hour_avg_2h", "element_sum_created_at_hour_percent_rank").alias("least_created_at_hour_avg_2h_element_sum_created_at_hour_percent_rank"),
        F.round("element_sum_created_at_hour_percent_rank", 1).alias("element_sum_created_at_hour_percent_rank_rounded"),
    )
)

result = (
    cleaned_carts
    .withColumn("element_sum_created_at_hour_percent_rank_2_tier", F.when(F.col("element_sum_created_at_hour_percent_rank_2") >= 3, "PREMIUM").otherwise("STANDARD"))
    .rollup("element_sum_created_at_hour_percent_rank_2_tier")
    .agg(
        F.percentile_approx("element_sum_created_at_hour_percent_rank_rounded", 0.9).alias("element_sum_created_at_hour_percent_rank_rounded_p90"),
        ((F.unix_timestamp(F.max("created_at")) - F.unix_timestamp(F.min("created_at"))) / 60).alias("span_minutes"),
        F.grouping_id().alias("grouping_id"),
    )
    .orderBy(F.col("span_minutes").asc_nulls_last())
)
