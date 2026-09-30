from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo_code").orderBy("starts_on", "id")
w2 = Window.orderBy("ends_on", "id").rowsBetween(-6, Window.currentRow)
w3 = Window.orderBy("seq_no", "id").rowsBetween(-2, -1)
w_recent = Window.orderBy("seq_no", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_promotions
    .filter(F.col("channel").isNotNull() & (F.col("starts_on") < "2025-04-06"))
    .select("*", F.date_sub("ends_on", 61).alias("ends_on_minus_61d"))
)

valid = (
    filtered
    .filter(F.col("starts_on") > F.lit("2023-03-20").cast("date"))
    .withColumn("starts_on_plus_months", F.add_months("starts_on", 3))
    .withColumn("channel_clean", F.coalesce("channel", F.lit("UNKNOWN")))
)

with_windows = (
    valid
    .select(
        "id",
        "starts_on",
        "ends_on",
        "ends_on_minus_61d",
        "starts_on_plus_months",
        "channel_clean",
        F.lead("discount_pct").over(w).alias("next_discount_pct"),
        F.row_number().over(w).alias("seq_no"),
    )
)

scored = (
    with_windows
    .filter(F.col("seq_no").isNotNull())
    .select(
        "id",
        "starts_on",
        "ends_on",
        "ends_on_minus_61d",
        "starts_on_plus_months",
        "seq_no",
        F.avg("seq_no").over(w2).alias("last7_avg_seq_no"),
        F.sum("seq_no").over(w2).alias("last7_sum_seq_no"),
        F.min("seq_no").over(w2).alias("last7_min_seq_no"),
        F.avg("seq_no").over(w3).alias("seq_no_avg_2_prior"),
    )
)

metrics = (
    scored
    .select(
        "*",
        F.count("last7_avg_seq_no").over(w_recent).alias("running_count_last7_avg_seq_no"),
        F.max("last7_avg_seq_no").over(w_recent).alias("running_max_last7_avg_seq_no"),
        F.min("last7_avg_seq_no").over(w_recent).alias("running_min_last7_avg_seq_no"),
        F.sum("last7_avg_seq_no").over(w_recent).alias("running_sum_last7_avg_seq_no"),
    )
)

result = (
    metrics
    .cube(
        F.date_trunc("YEAR", "starts_on_plus_months").alias("year_start"),
        F.date_trunc("WEEK", "starts_on_plus_months").alias("week_start"),
    )
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.percentile_approx("seq_no_avg_2_prior", [0.25, 0.5, 0.75]).alias("seq_no_avg_2_prior_quartiles"),
        F.countDistinct("id").alias("distinct_ids_2"),
        F.percentile_approx("running_max_last7_avg_seq_no", [0.25, 0.5, 0.75]).alias("running_max_last7_avg_seq_no_quartiles"),
    )
    .filter(F.col("distinct_ids_2") > 1)
    .orderBy(F.col("distinct_ids").desc())
)
