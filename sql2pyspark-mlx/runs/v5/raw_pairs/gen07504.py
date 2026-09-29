from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel").orderBy("starts_on")
w_grp = Window.partitionBy("channel").orderBy("ends_on", "id")
w3 = Window.partitionBy("promo_code").orderBy("ends_on", "id")
w_part = Window.partitionBy("channel").orderBy("prev_discount_pct", "id")
w5 = Window.partitionBy("channel").orderBy("next_discount_pct_2", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_recent = Window.orderBy("next_discount_pct_2", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.orderBy("event_seq", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.orderBy("event_seq_2", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

deduped = (
    df_promotions
    .withColumn("dedup_rank", F.rank().over(w))
    .filter(F.col("dedup_rank") == 1)
    .drop("dedup_rank")
)

scored = (
    deduped
    .select(
        "id",
        "channel",
        F.lead("discount_pct").over(w_grp).alias("next_discount_pct"),
        F.lag("discount_pct").over(w_grp).alias("prev_discount_pct"),
        F.row_number().over(w_grp).alias("event_seq"),
        F.lead("discount_pct", 1).over(w3).alias("next_discount_pct_2"),
    )
)

with_windows = (
    scored
    .select(
        "id",
        "event_seq",
        "next_discount_pct_2",
        F.row_number().over(w_part).alias("event_seq_2"),
        F.sum("prev_discount_pct").over(w5).alias("running_prev_discount_pct"),
    )
)

scored_promotions = (
    with_windows
    .withColumn("running_max_next_discount_pct_2", F.max("next_discount_pct_2").over(w_recent))
    .withColumn("running_count_next_discount_pct_2", F.count("next_discount_pct_2").over(w_recent))
    .withColumn("running_avg_next_discount_pct_2", F.avg("next_discount_pct_2").over(w_recent))
    .withColumn("running_min_next_discount_pct_2", F.min("next_discount_pct_2").over(w_recent))
    .withColumn("first_known_event_seq_2", F.first("event_seq_2", ignorenulls=True).over(w7))
)

base = (
    scored_promotions
    .filter(F.col("running_max_next_discount_pct_2") < 0.17)
    .select(
        "id",
        "next_discount_pct_2",
        "event_seq_2",
        "running_max_next_discount_pct_2",
        "running_count_next_discount_pct_2",
        "running_avg_next_discount_pct_2",
        "running_min_next_discount_pct_2",
        "first_known_event_seq_2",
        F.round((F.col("event_seq") / F.col("next_discount_pct_2")), 2).alias("event_seq_per_next_discount_pct_2"),
    )
)

metrics = (
    base
    .select(
        "id",
        "event_seq_2",
        "running_max_next_discount_pct_2",
        "running_avg_next_discount_pct_2",
        F.sum("running_max_next_discount_pct_2").over(w8).alias("running_sum_running_max_next_discount_pct_2"),
        F.max("running_max_next_discount_pct_2").over(w8).alias("running_max_running_max_next_discount_pct_2"),
        F.avg("running_max_next_discount_pct_2").over(w8).alias("running_avg_running_max_next_discount_pct_2"),
        F.count("running_max_next_discount_pct_2").over(w8).alias("running_count_running_max_next_discount_pct_2"),
    )
)

result = (
    metrics
    .select(
        "id",
        "running_max_next_discount_pct_2",
        "running_avg_next_discount_pct_2",
        "running_sum_running_max_next_discount_pct_2",
        "running_count_running_max_next_discount_pct_2",
    )
    .orderBy(F.col("running_max_next_discount_pct_2").asc_nulls_last())
)
