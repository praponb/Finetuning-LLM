from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("value_length")
w2 = Window.partitionBy("value")
w3 = Window.partitionBy("value").orderBy(F.col("created_at").cast("bigint")).rangeBetween(-3600, Window.currentRow)
w4 = Window.orderBy(F.col("value_sum_value_length_2").desc_nulls_first())
w5 = Window.partitionBy("value").orderBy(F.col("created_at").cast("bigint")).rangeBetween(-7200, Window.currentRow)
w6 = Window.orderBy(F.col("created_at").cast("bigint")).rangeBetween(-1800, Window.currentRow)
w7 = Window.partitionBy("value").orderBy(F.col("value_length_cume_dist").desc_nulls_first())
w8 = Window.partitionBy("value").orderBy(F.col("created_at").cast("bigint")).rangeBetween(-1800, Window.currentRow)
w9 = Window.partitionBy("value").orderBy(F.col("value_sum_value_length_3_count_30m").desc_nulls_last())

exploded = df_carts.select("id", "created_at", F.explode_outer("coupon_codes").alias("value"))

filtered = (
    exploded
    .filter(~F.col("value").eqNullSafe(F.lit("BUNDLE")))
    .select(
        "*",
        F.length("value").alias("value_length"),
        F.upper("value").alias("value_upper"),
        F.coalesce("value", F.lit("OTHER")).alias("value_filled"),
    )
)

with_windows = (
    filtered
    .filter(F.col("created_at") < "2024-05-01")
    .withColumn("value_length_cume_dist", F.cume_dist().over(w))
    .withColumn("value_sum_value_length", F.sum("value_length").over(w2))
    .withColumn("value_sum_value_length_2", F.sum("value_length").over(w2))
    .withColumn("value_sum_value_length_3", F.sum("value_length").over(w2))
    .withColumn("value_length_sum_1h", F.sum("value_length").over(w3))
)

with_windows_carts = (
    with_windows
    .withColumn("value_sum_value_length_2_percent_rank", F.percent_rank().over(w4))
    .withColumn("value_sum_value_length_3_max_2h", F.max("value_sum_value_length_3").over(w5))
    .withColumn("value_sum_value_length_3_count_30m", F.count("value_sum_value_length_3").over(w6))
    .withColumn("value_avg_value_sum_value_length_3", F.avg("value_sum_value_length_3").over(w2))
)

metrics = (
    with_windows_carts
    .select(
        "created_at",
        "value_length_cume_dist",
        "value_sum_value_length_2",
        "value_length_sum_1h",
        "value_sum_value_length_2_percent_rank",
        "value_sum_value_length_3_max_2h",
        "value_avg_value_sum_value_length_3",
        F.rank().over(w7).alias("value_length_cume_dist_rank"),
        F.count("value_sum_value_length_3").over(w8).alias("value_sum_value_length_3_count_30m_2"),
        F.rank().over(w9).alias("value_sum_value_length_3_count_30m_rank"),
    )
)

result = (
    metrics
    .groupBy(
        F.when(F.col("value_avg_value_sum_value_length_3") >= 440, "HIGH").otherwise("LOW").alias("value_avg_value_sum_value_length_3_tier"),
    )
    .agg(
        F.percentile_approx("value_sum_value_length_3_max_2h", 0.5).alias("value_sum_value_length_3_max_2h_p50"),
        ((F.unix_timestamp(F.max("created_at")) - F.unix_timestamp(F.min("created_at"))) / 60).alias("duration_min"),
        F.percentile_approx("value_avg_value_sum_value_length_3", [0.25, 0.5, 0.75]).alias("value_avg_value_sum_value_length_3_quartiles"),
    )
    .orderBy(F.col("value_sum_value_length_3_max_2h_p50").asc_nulls_first())
)
