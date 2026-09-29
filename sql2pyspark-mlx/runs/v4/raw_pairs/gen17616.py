from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.partitionBy("key").orderBy("created_at", "id", "key")
w2 = Window.partitionBy("key").orderBy("created_at", "id", "key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("key").orderBy("seq_no", "id", "key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("key").orderBy("new_group_flag", "id", "key")
w5 = Window.partitionBy("key").orderBy("island_id")
w6 = Window.orderBy("new_group_flag", "id", "key")
w7 = Window.partitionBy("key").orderBy("cumulative_seq_no_2", "id", "key")
w8 = Window.partitionBy("key").orderBy("new_group_flag", "id", "key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w9 = Window.partitionBy("key").orderBy("island_id_percent_rank", "id", "key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_hist = Window.partitionBy("key").orderBy(F.col("created_at").cast("bigint")).rangeBetween(-900, Window.currentRow)
w11 = Window.partitionBy("key")

lines = (
    df_carts
    .select(
        "id",
        "created_at",
        "coupon_codes",
        F.explode("attributes").alias("key", "attr_value"),
    )
)

with_windows = (
    lines
    .select(
        "*",
        F.row_number().over(w_part).alias("seq_no"),
        F.row_number().over(w_part).alias("seq_no_2"),
    )
)

with_gaps = (
    with_windows
    .withColumn("new_group_flag", F.when(F.lag("created_at").over(w_part).isNull() | (F.col("created_at") > F.lag("created_at").over(w_part) + F.expr("INTERVAL 45 MINUTES")), 1).otherwise(0))
)

sessionized = with_gaps.withColumn("island_id", F.sum("new_group_flag").over(w2))

scored = (
    sessionized
    .withColumn("running_new_group_flag", F.sum("new_group_flag").over(w2))
    .withColumn("cumulative_seq_no_2", F.sum("seq_no_2").over(w3))
    .withColumn("next_island_id", F.lead("island_id", 2, 0).over(w4))
    .withColumn("island_id_percent_rank", F.percent_rank().over(w5))
    .withColumn("prev_seq_no_2", F.lag("seq_no_2", 1).over(w6))
)

metrics = (
    scored
    .withColumn("next_cumulative_seq_no_2", F.lead("cumulative_seq_no_2", 1).over(w7))
    .withColumn("first_known_next_island_id", F.first("next_island_id", ignorenulls=True).over(w8))
    .withColumn("first_known_cumulative_seq_no_2", F.first("cumulative_seq_no_2", ignorenulls=True).over(w3))
    .withColumn("last_known_key", F.last("key", ignorenulls=True).over(w9))
    .withColumn("cumulative_prev_seq_no_2", F.sum("prev_seq_no_2").over(w3))
)

metrics_carts = (
    metrics
    .select(
        "*",
        F.sum("seq_no").over(w_hist).alias("seq_no_sum_15m"),
        F.sum("seq_no_2").over(w11).alias("key_sum_seq_no_2"),
    )
)

result = (
    metrics_carts
    .rollup(
        F.when(F.col("cumulative_prev_seq_no_2") >= 32, "PREMIUM").otherwise("STANDARD").alias("cumulative_prev_seq_no_2_tier"),
        F.when(F.col("new_group_flag") >= 1, "PREMIUM").otherwise("STANDARD").alias("new_group_flag_tier"),
        F.when(F.col("first_known_next_island_id") >= 8, "LARGE").otherwise("SMALL").alias("first_known_next_island_id_tier"),
    )
    .agg(
        F.round(F.avg("next_island_id"), 2).alias("avg_next_island_id"),
        F.max(F.when(F.col("key") == "weight_kg", F.col("new_group_flag"))).alias("weight_kg_max_new_group_flag"),
        F.countDistinct("id").alias("distinct_ids"),
        F.round(F.sum("island_id_percent_rank") / F.nullif(F.count(F.when(F.col("key") == "weight_kg", 1)), F.lit(0)), 2).alias("island_id_percent_rank_per_weight_kg"),
        F.percentile_approx("island_id", 0.9).alias("island_id_p90"),
    )
    .filter(F.col("island_id_percent_rank_per_weight_kg") >= 0)
    .orderBy(F.col("island_id_p90").desc_nulls_last())
)
