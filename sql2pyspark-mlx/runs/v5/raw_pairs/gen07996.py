from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy("rating_adjusted", "id").rowsBetween(-2, Window.currentRow)
w2 = Window.partitionBy("city").orderBy(F.col("rating_adjusted").desc_nulls_last())
w3 = Window.partitionBy("city").orderBy("rating_adjusted", "id")
w4 = Window.orderBy("rating_adjusted_rank", "id").rowsBetween(-2, -1)
w_running = Window.orderBy("rating_adjusted_avg_2", "id")
w6 = Window.orderBy("rating_adjusted_rank", "id")
w7 = Window.orderBy("event_seq_avg_2_prior", "id")
w8 = Window.partitionBy("prev_rating_adjusted_rank_tier").orderBy(F.col("max_row_seq").desc())

base = (
    df_suppliers
    .filter(F.col("city").isNotNull() & (F.col("city").eqNullSafe(F.lit("Singapore"))))
    .select(
        "id",
        "supplier_name",
        "city",
        F.round((F.col("rating") * 1.07), 2).alias("rating_adjusted"),
    )
)

enriched = (
    base
    .select(
        "id",
        "supplier_name",
        "rating_adjusted",
        F.avg("rating_adjusted").over(w).alias("rating_adjusted_avg_2"),
        F.rank().over(w2).alias("rating_adjusted_rank"),
        F.row_number().over(w3).alias("event_seq"),
    )
)

scored = (
    enriched
    .select(
        "*",
        F.avg("event_seq").over(w4).alias("event_seq_avg_2_prior"),
        F.lag("rating_adjusted_avg_2", 2).over(w_running).alias("prev_rating_adjusted_avg_2"),
        F.ntile(10).over(w6).alias("rating_adjusted_rank_bucket"),
    )
)

with_windows = (
    scored
    .withColumn("next_prev_rating_adjusted_avg_2", F.lead("prev_rating_adjusted_avg_2").over(w7))
    .withColumn("next_rating_adjusted_rank", F.lead("rating_adjusted_rank").over(w7))
    .withColumn("row_seq", F.row_number().over(w7))
    .withColumn("prev_rating_adjusted_rank", F.lag("rating_adjusted_rank", 1).over(w7))
)

summary = (
    with_windows
    .groupBy(
        F.when(F.col("prev_rating_adjusted_rank") >= 7, "PREMIUM").otherwise("STANDARD").alias("prev_rating_adjusted_rank_tier"),
        F.when(F.col("event_seq_avg_2_prior") >= 6, "LARGE").otherwise("SMALL").alias("event_seq_avg_2_prior_tier"),
        F.when(F.col("next_prev_rating_adjusted_avg_2") >= 74, "LARGE").otherwise("SMALL").alias("next_prev_rating_adjusted_avg_2_tier"),
    )
    .agg(
        F.percentile_approx("rating_adjusted_rank", [0.25, 0.5, 0.75]).alias("rating_adjusted_rank_quartiles"),
        F.countDistinct("id").alias("distinct_ids"),
        F.max("row_seq").alias("max_row_seq"),
        F.count("*").alias("event_count"),
        F.sum("event_seq_avg_2_prior").alias("total_event_seq_avg_2_prior"),
        F.percentile_approx("row_seq", [0.25, 0.5, 0.75]).alias("row_seq_quartiles"),
    )
    .withColumn("max_row_seq_percent_rank", F.percent_rank().over(w8))
)

result = (
    summary
    .select(
        "prev_rating_adjusted_rank_tier",
        "event_seq_avg_2_prior_tier",
        "next_prev_rating_adjusted_avg_2_tier",
        "rating_adjusted_rank_quartiles",
        "distinct_ids",
        "max_row_seq",
        "row_seq_quartiles",
        "max_row_seq_percent_rank",
    )
)
