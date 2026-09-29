from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.partitionBy("promo_code").orderBy("discount_pct", "id")
w2 = Window.partitionBy("channel")
w3 = Window.partitionBy("channel").orderBy("discount_pct", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("promo_code")
w5 = Window.orderBy("running_avg_next_discount_pct")
w6 = Window.orderBy("running_count_next_discount_pct_num", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.orderBy("cumulative_running_count_next_discount_pct", "id")
w8 = Window.orderBy("running_avg_next_discount_pct_rank", "id").rowsBetween(-6, -1)
w9 = Window.partitionBy("seq_no").orderBy(F.col("num_rows").desc())

cleaned = (
    df_promotions
    .filter(F.year("ends_on") == 2023)
    .select(
        "id",
        "channel",
        "promo_code",
        "discount_pct",
        F.trim("channel").alias("clean_channel"),
    )
)

with_windows = (
    cleaned
    .filter(F.col("discount_pct") > 0.3)
    .withColumn("next_discount_pct", F.lead("discount_pct").over(w_recent))
    .withColumn("prev_discount_pct", F.lag("discount_pct").over(w_recent))
    .withColumn("event_seq", F.row_number().over(w_recent))
    .withColumn("channel_count_discount_pct", F.count("discount_pct").over(w2))
)

with_windows_promotions = (
    with_windows
    .select(
        "id",
        "channel",
        "discount_pct",
        "event_seq",
        F.count("next_discount_pct").over(w3).alias("running_count_next_discount_pct"),
        F.sum("next_discount_pct").over(w3).alias("running_sum_next_discount_pct"),
        F.avg("next_discount_pct").over(w3).alias("running_avg_next_discount_pct"),
        F.avg("event_seq").over(w4).alias("promo_code_avg_event_seq"),
    )
)

enriched = (
    with_windows_promotions
    .select(
        "id",
        "running_count_next_discount_pct",
        "running_sum_next_discount_pct",
        "promo_code_avg_event_seq",
        F.sum("running_count_next_discount_pct").over(w3).alias("cumulative_running_count_next_discount_pct"),
        F.rank().over(w5).alias("running_avg_next_discount_pct_rank"),
    )
)

valid = (
    enriched
    .filter(F.col("running_sum_next_discount_pct").between(1.46, 1.54))
    .select(
        "*",
        F.col("running_count_next_discount_pct").cast("double").alias("running_count_next_discount_pct_num"),
        (F.col("running_sum_next_discount_pct") * F.col("promo_code_avg_event_seq")).alias("running_sum_next_discount_pct_times_promo_code_avg_event_seq"),
    )
)

scored = (
    valid
    .select(
        "*",
        F.sum("running_sum_next_discount_pct").over(w6).alias("cumulative_running_sum_next_discount_pct"),
        F.lead("running_avg_next_discount_pct_rank", 1).over(w7).alias("next_running_avg_next_discount_pct_rank"),
        F.row_number().over(w7).alias("seq_no"),
        F.avg("promo_code_avg_event_seq").over(w8).alias("promo_code_avg_event_seq_avg_6_prior"),
    )
)

result = (
    scored
    .groupBy(
        "seq_no",
        F.when(F.col("cumulative_running_count_next_discount_pct") >= 85, "HIGH").otherwise("LOW").alias("cumulative_running_count_next_discount_pct_tier"),
        F.when(F.col("running_avg_next_discount_pct_rank") >= 8, "HIGH").otherwise("LOW").alias("running_avg_next_discount_pct_rank_tier"),
    )
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.percentile_approx("running_avg_next_discount_pct_rank", [0.25, 0.5, 0.75]).alias("running_avg_next_discount_pct_rank_quartiles"),
        F.count("*").alias("num_rows"),
        F.percentile_approx("cumulative_running_sum_next_discount_pct", 0.95).alias("cumulative_running_sum_next_discount_pct_p95"),
        F.round(F.avg("running_sum_next_discount_pct_times_promo_code_avg_event_seq"), 2).alias("avg_running_sum_next_discount_pct_times_promo_code_avg_event_seq"),
        F.min("running_avg_next_discount_pct_rank").alias("min_running_avg_next_discount_pct_rank"),
    )
    .withColumn("num_rows_dense_rank", F.dense_rank().over(w9))
)
